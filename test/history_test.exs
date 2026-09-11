defmodule Group.HistoryTest do
  use ExUnit.Case, async: true

  @moduletag :local
  @moduletag :capture_log
  @moduletag :history

  # ExUnit seeds :rand per test. The complete generated input is retained in CI,
  # so failures can be replayed with --seed, even when the campaign is larger.
  for shards <- [1, 4, 8] do
    @tag shards: shards
    test "registry and PG histories match a reference model with #{shards} shards", context do
      name = :"history_#{System.unique_integer([:positive])}"
      start_supervised!({Group, name: name, shards: context.shards, log: false})
      steps = String.to_integer(System.get_env("GROUP_HISTORY_STEPS", "100"))
      assert steps in 1..100_000

      history =
        for step <- 1..steps do
          {Enum.random([:register, :unregister, :join, :leave, :disconnect]),
           Enum.random([nil, "red", "blue"]), "key/#{Enum.random(1..4)}", %{step: step}}
        end

      Group.TestDiagnostics.record(:history, %{
        test: context.test,
        shards: context.shards,
        seed: ExUnit.configuration()[:seed],
        operations: history
      })

      Enum.reduce(history, {%{}, %{}}, fn operation, model ->
        model = apply_operation(name, operation, model)
        assert_model(name, model, operation)
        model
      end)
    end
  end

  defp apply_operation(_name, {:disconnect, nil, _key, _meta}, model), do: model

  defp apply_operation(name, {:disconnect, cluster, _key, _meta}, {registry, groups}) do
    :ok = Group.disconnect(name, cluster)
    keep? = fn {{entry_cluster, _key}, _meta} -> entry_cluster != cluster end
    {Map.filter(registry, keep?), Map.filter(groups, keep?)}
  end

  defp apply_operation(name, {operation, cluster, key, meta}, {registry, groups}) do
    if cluster, do: Group.connect(name, cluster)
    opts = [cluster: cluster]
    entry = {cluster, key}

    case operation do
      :register ->
        :ok = Group.register(name, key, meta, opts)
        {Map.put(registry, entry, meta), groups}

      :unregister ->
        expected = if Map.has_key?(registry, entry), do: :ok, else: {:error, :undefined}
        assert Group.unregister(name, key, opts) == expected
        {Map.delete(registry, entry), groups}

      :join ->
        :ok = Group.join(name, key, meta, opts)
        {registry, Map.put(groups, entry, meta)}

      :leave ->
        expected = if Map.has_key?(groups, entry), do: :ok, else: {:error, :not_in_group}
        assert Group.leave(name, key, opts) == expected
        {registry, Map.delete(groups, entry)}
    end
  end

  defp assert_model(name, {registry, groups}, operation) do
    for cluster <- [nil, "red", "blue"], key <- 1..4 do
      key = "key/#{key}"
      entry = {cluster, key}
      expected_registration = if meta = registry[entry], do: {self(), meta}
      expected_members = if meta = groups[entry], do: [{self(), meta}], else: []

      assert Group.lookup(name, key, cluster: cluster) == expected_registration,
             "registry mismatch after #{inspect(operation)}"

      assert Group.members(name, key, cluster: cluster) == expected_members,
             "PG mismatch after #{inspect(operation)}"
    end
  end
end
