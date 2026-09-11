defmodule Group.TestDiagnostics do
  @moduledoc false
  use GenServer

  def init(opts), do: {:ok, opts}

  def handle_cast({:suite_started, opts}, state) do
    record(:suite, %{
      options: opts,
      elixir: System.version(),
      otp: System.otp_release(),
      schedulers: :erlang.system_info(:schedulers_online),
      peer_schedulers: System.get_env("GROUP_PEER_SCHEDULERS", "2"),
      history_steps: System.get_env("GROUP_HISTORY_STEPS", "100")
    })

    {:noreply, state}
  end

  def handle_cast({:test_finished, %ExUnit.Test{state: {:failed, _}} = test}, state) do
    record(:failure, test)
    {:noreply, state}
  end

  def handle_cast(_event, state), do: {:noreply, state}

  def record(kind, data) do
    if directory = System.get_env("GROUP_TEST_DIAGNOSTICS") do
      File.mkdir_p!(directory)
      id = "#{System.pid()}-#{System.unique_integer([:positive, :monotonic])}"
      path = Path.join(directory, "#{id}-#{kind}.txt")
      File.write!(path, inspect(data, pretty: true, limit: :infinity, printable_limit: :infinity))
    end

    :ok
  end

  def capture_peers(peers) do
    if System.get_env("GROUP_TEST_DIAGNOSTICS") do
      for {_pid, peer} <- peers do
        snapshot =
          try do
            :erpc.call(peer, __MODULE__, :snapshot, [], 2_000)
          catch
            kind, reason -> %{unavailable: {kind, reason}}
          end

        record(:peer_snapshot, %{node: peer, snapshot: snapshot})
      end
    end
  end

  # Called before stopping fresh peers, including during failure/timeout cleanup.
  # Bounded sys calls preserve evidence without turning a stuck shard into a hang.
  def snapshot do
    groups =
      for {{Group, name}, config} <- :persistent_term.get(),
          is_map(config),
          is_integer(config[:num_shards]) do
        shards =
          for index <- 0..(config.num_shards - 1) do
            shard = Group.Replica.shard_name(name, index)
            pid = Process.whereis(shard)

            state =
              try do
                :sys.get_state(shard, 100)
                |> inspect(pretty: true, limit: 100, printable_limit: 8_000)
              catch
                kind, reason -> {kind, reason}
              end

            %{
              shard: shard,
              state: state,
              process: pid && Process.info(pid, [:status, :current_function, :message_queue_len])
            }
          end

        %{name: name, config: config, shards: shards}
      end

    %{
      node: node(),
      connected_nodes: Node.list(),
      schedulers: :erlang.system_info(:schedulers_online),
      groups: groups,
      tables:
        Enum.map(:ets.all(), fn table ->
          :ets.info(table)
          |> case do
            :undefined -> :deleted
            info -> Keyword.take(info, [:name, :size, :memory, :owner])
          end
        end)
    }
  end
end
