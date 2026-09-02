defmodule Group.CyclicConflictResolver do
  @moduledoc false

  # New contract: rank one claim. Erlang term ordering plus Group's PID
  # tiebreaker makes max/2 associative and independent of arrival order.
  def resolve(_name, _key, {_pid, %{rank: rank}, _time}), do: rank

  # Old contract: a deterministic but cyclic pairwise choice. This exists only
  # so the regression demonstrates why arbitrary pairwise resolvers are unsafe.
  def resolve(_name, _key, {pid1, %{rank: rank1}, _time1}, {pid2, %{rank: rank2}, _time2}) do
    if beats?(rank1, rank2), do: pid1, else: pid2
  end

  defp beats?(:a, :b), do: true
  defp beats?(:b, :c), do: true
  defp beats?(:c, :a), do: true
  defp beats?(rank, rank), do: true
  defp beats?(_left, _right), do: false
end
