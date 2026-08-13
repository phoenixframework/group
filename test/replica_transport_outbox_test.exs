defmodule Group.ReplicaTransportOutboxTest do
  use ExUnit.Case, async: true

  alias Group.Transport.Outbox

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
    def send_batch(target_node, messages, deadline, state) do
      send(
        state.controller,
        {:outbox_batch, state.group, state.shard, target_node, messages, deadline}
      )

      if state.sleep > 0, do: Process.sleep(state.sleep)
      {state.result, state}
    end
  end

  test "batches messages per target while preserving per-target order" do
    group = unique_group(:batch)
    target_a = :"outbox-a@test"
    target_b = :"outbox-b@test"

    start_outboxes(group,
      outbox_batch_size: 3,
      outbox_flush_interval: 1_000
    )

    assert :ok = Outbox.push(group, target_a, 0, {:message, 1}, outbox_deadline: 1_000)
    assert :ok = Outbox.push(group, target_b, 0, {:message, 2}, outbox_deadline: 1_000)
    assert :ok = Outbox.push(group, target_a, 0, {:message, 3}, outbox_deadline: 1_000)

    batches =
      for _ <- 1..2, into: %{} do
        assert_receive {:outbox_batch, ^group, 0, target, messages, deadline}, 1_000
        assert deadline > Outbox.monotonic_ms()
        {target, messages}
      end

    assert batches == %{
             target_a => [{:message, 1}, {:message, 3}],
             target_b => [{:message, 2}]
           }
  end

  test "a blocked backend never blocks the Group-facing local send" do
    group = unique_group(:blocked)
    target = :"outbox-blocked@test"

    start_outboxes(group,
      outbox_batch_size: 1,
      backend_sleep: 200
    )

    assert :ok = Outbox.push(group, target, 0, :first, outbox_deadline: 1_000)
    assert_receive {:outbox_batch, ^group, 0, ^target, [:first], _deadline}, 1_000

    caller = self()

    spawn(fn ->
      result = Outbox.push(group, target, 0, :expires_behind_backend, outbox_deadline: 10)
      send(caller, {:push_returned, result})
    end)

    assert_receive {:push_returned, :ok}, 100
    refute_receive {:outbox_batch, ^group, 0, ^target, [:expires_behind_backend], _deadline}, 300
  end

  test "expired messages and backend backpressure are dropped without local retries" do
    expired_group = unique_group(:expired)
    target = :"outbox-expired@test"

    start_outboxes(expired_group,
      outbox_flush_interval: 50
    )

    assert :ok =
             Outbox.push(expired_group, target, 0, :expired, outbox_deadline: 5)

    refute_receive {:outbox_batch, ^expired_group, 0, ^target, [:expired], _deadline}, 100

    busy_group = unique_group(:busy)

    start_outboxes(busy_group,
      outbox_batch_size: 1,
      backend_result: :busy
    )

    assert :ok = Outbox.push(busy_group, target, 0, :busy, outbox_deadline: 1_000)
    assert_receive {:outbox_batch, ^busy_group, 0, ^target, [:busy], _deadline}, 1_000
    refute_receive {:outbox_batch, ^busy_group, 0, ^target, [:busy], _deadline}, 100
  end

  test "complete incoming batches use one local mailbox operation" do
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
             Group.Transport.incoming_batch(
               group,
               source_node,
               0,
               [{:heads, 1, []}, {:needs, 1, []}]
             )

    assert_receive {:receiver_message,
                    {:group_replica_batch, ^source_node, [{:heads, 1, []}, {:needs, 1, []}]}}

    refute Process.alive?(receiver)
  end

  test "incoming messages are dropped when the destination shard is unavailable" do
    group = unique_group(:missing_ingress)
    source_node = :"missing-ingress@test"

    assert :disconnected = Group.Transport.incoming(group, source_node, 0, {:heads, 1, []})

    assert :disconnected =
             Group.Transport.incoming_batch(group, source_node, 0, [
               {:heads, 1, []},
               {:needs, 1, []}
             ])
  end

  test "invalid outbox deadlines are rejected when the outbox supervisor boots" do
    group = unique_group(:invalid_deadline)

    assert_raise ArgumentError, ~r/:outbox_deadline/, fn ->
      Group.Transport.Outbox.Supervisor.init(
        name: group,
        num_shards: 1,
        backend: Backend,
        controller: self(),
        outbox_deadline: 0
      )
    end
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
