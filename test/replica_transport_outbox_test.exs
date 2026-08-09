defmodule Group.ReplicaTransportOutboxTest do
  use ExUnit.Case, async: true

  alias Group.Replica.Transport.Outbox

  defmodule Backend do
    @behaviour Outbox

    @impl true
    def init_outbox(group, shard, opts) do
      {:ok,
       %{
         controller: Keyword.fetch!(opts, :controller),
         group: group,
         shard: shard,
         result: Keyword.get(opts, :backend_result, :ok),
         sleep: Keyword.get(opts, :backend_sleep, 0)
       }}
    end

    @impl true
    def send_batch(target_node, frames, deadline, state) do
      send(
        state.controller,
        {:outbox_batch, state.group, state.shard, target_node, frames, deadline}
      )

      if state.sleep > 0, do: Process.sleep(state.sleep)
      {state.result, state}
    end
  end

  test "batches frames per target while preserving per-target order" do
    group = unique_group(:batch)
    target_a = :"outbox-a@test"
    target_b = :"outbox-b@test"

    start_outboxes(group,
      outbox_batch_size: 3,
      outbox_flush_interval: 1_000
    )

    assert :ok = Outbox.try_send(group, target_a, 0, {:frame, 1}, outbox_deadline: 1_000)
    assert :ok = Outbox.try_send(group, target_b, 0, {:frame, 2}, outbox_deadline: 1_000)
    assert :ok = Outbox.try_send(group, target_a, 0, {:frame, 3}, outbox_deadline: 1_000)

    batches =
      for _ <- 1..2, into: %{} do
        assert_receive {:outbox_batch, ^group, 0, target, frames, deadline}, 1_000
        assert deadline > Outbox.monotonic_ms()
        {target, frames}
      end

    assert batches == %{
             target_a => [{:frame, 1}, {:frame, 3}],
             target_b => [{:frame, 2}]
           }
  end

  test "a blocked backend never blocks the Group-facing local send" do
    group = unique_group(:blocked)
    target = :"outbox-blocked@test"

    start_outboxes(group,
      outbox_batch_size: 1,
      backend_sleep: 200
    )

    assert :ok = Outbox.try_send(group, target, 0, :first, outbox_deadline: 1_000)
    assert_receive {:outbox_batch, ^group, 0, ^target, [:first], _deadline}, 1_000

    caller = self()

    spawn(fn ->
      result = Outbox.try_send(group, target, 0, :expires_behind_backend, outbox_deadline: 10)
      send(caller, {:try_send_returned, result})
    end)

    assert_receive {:try_send_returned, :ok}, 100
    refute_receive {:outbox_batch, ^group, 0, ^target, [:expires_behind_backend], _deadline}, 300
  end

  test "expired frames and backend backpressure are dropped without local retries" do
    expired_group = unique_group(:expired)
    target = :"outbox-expired@test"

    start_outboxes(expired_group,
      outbox_flush_interval: 50
    )

    assert :ok =
             Outbox.try_send(expired_group, target, 0, :expired, outbox_deadline: 5)

    refute_receive {:outbox_batch, ^expired_group, 0, ^target, [:expired], _deadline}, 100

    busy_group = unique_group(:busy)

    start_outboxes(busy_group,
      outbox_batch_size: 1,
      backend_result: :busy
    )

    assert :ok = Outbox.try_send(busy_group, target, 0, :busy, outbox_deadline: 1_000)
    assert_receive {:outbox_batch, ^busy_group, 0, ^target, [:busy], _deadline}, 1_000
    refute_receive {:outbox_batch, ^busy_group, 0, ^target, [:busy], _deadline}, 100
  end

  test "complete inbound batches use one authenticated local delivery" do
    group = unique_group(:deliver)
    source_node = :"outbox-source@test"
    parent = self()
    shard_name = Group.Replica.shard_name(group, 0)

    receiver =
      spawn(fn ->
        Process.register(self(), shard_name)
        send(parent, :receiver_ready)

        receive do
          message -> send(parent, {:receiver_message, message})
        end
      end)

    assert_receive :receiver_ready

    assert :ok =
             Group.Replica.Transport.deliver_batch(
               group,
               source_node,
               0,
               [{:heads, 1, []}, {:needs, 1, []}]
             )

    assert_receive {:receiver_message,
                    {:group_replica_batch, ^source_node, [{:heads, 1, []}, {:needs, 1, []}]}}

    refute Process.alive?(receiver)
  end

  defp start_outboxes(group, opts) do
    base = [
      name: group,
      num_shards: 1,
      backend: Backend,
      controller: self(),
      outbox_deadline: 100
    ]

    start_supervised!(Outbox.child_spec(Keyword.merge(base, opts)))
  end

  defp unique_group(suffix) do
    :"outbox_#{suffix}_#{System.unique_integer([:positive])}"
  end
end
