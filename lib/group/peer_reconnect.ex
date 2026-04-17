defmodule Group.PeerReconnect do
  @moduledoc false

  use GenServer

  require Logger

  defstruct [
    :name,
    :retry_attempts,
    :retry_interval,
    retrying: %{}
  ]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    retry_attempts = Keyword.fetch!(opts, :busy_dist_retry_attempts)
    retry_interval = Keyword.fetch!(opts, :busy_dist_retry_interval)

    GenServer.start_link(
      __MODULE__,
      {name, retry_attempts, retry_interval},
      name: reconnect_name(name)
    )
  end

  @doc false
  def reconnect_name(name), do: :"#{name}_peer_reconnect"

  @doc false
  def busy_link(name, remote_node) when is_atom(name) and is_atom(remote_node) do
    case GenServer.whereis(reconnect_name(name)) do
      nil ->
        :erlang.disconnect_node(remote_node)
        :ok

      pid ->
        GenServer.cast(pid, {:busy_link, remote_node})
    end
  end

  @impl true
  def init({name, retry_attempts, retry_interval}) do
    :net_kernel.monitor_nodes(true)

    {:ok,
     %__MODULE__{
       name: name,
       retry_attempts: retry_attempts,
       retry_interval: retry_interval
     }}
  end

  @impl true
  def handle_cast({:busy_link, remote_node}, state) when remote_node == node() do
    {:noreply, state}
  end

  def handle_cast({:busy_link, remote_node}, state) do
    if Map.has_key?(state.retrying, remote_node) do
      {:noreply, state}
    else
      Logger.warning(
        "[group #{inspect(state.name)}] busy dist link to #{inspect(remote_node)}; disconnecting and starting reconnect retries"
      )

      :erlang.disconnect_node(remote_node)
      {:noreply, schedule_retry(state, remote_node, state.retry_attempts)}
    end
  end

  @impl true
  def handle_info({:retry_connect, remote_node}, state) do
    case state.retrying do
      %{^remote_node => %{attempts_left: attempts_left}} ->
        cond do
          remote_node in Node.list() ->
            {:noreply, clear_retry(state, remote_node)}

          Node.connect(remote_node) ->
            {:noreply, clear_retry(state, remote_node)}

          attempts_left <= 1 ->
            Logger.warning(
              "[group #{inspect(state.name)}] giving up reconnect retries to #{inspect(remote_node)} after busy dist disconnect"
            )

            {:noreply, clear_retry(state, remote_node)}

          true ->
            {:noreply, schedule_retry(state, remote_node, attempts_left - 1)}
        end

      %{} ->
        {:noreply, state}
    end
  end

  def handle_info({:nodeup, remote_node}, state) do
    {:noreply, clear_retry(state, remote_node)}
  end

  def handle_info({:nodedown, _remote_node}, state) do
    {:noreply, state}
  end

  defp schedule_retry(state, _remote_node, 0), do: state

  defp schedule_retry(state, remote_node, attempts_left) do
    timer_ref = Process.send_after(self(), {:retry_connect, remote_node}, state.retry_interval)

    put_in(
      state.retrying[remote_node],
      %{attempts_left: attempts_left, timer_ref: timer_ref}
    )
  end

  defp clear_retry(state, remote_node) do
    case Map.pop(state.retrying, remote_node) do
      {nil, retrying} ->
        %{state | retrying: retrying}

      {%{timer_ref: timer_ref}, retrying} ->
        Process.cancel_timer(timer_ref, async: true, info: false)
        %{state | retrying: retrying}
    end
  end
end
