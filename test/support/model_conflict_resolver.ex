defmodule Group.ModelConflictResolver do
  @moduledoc false

  def resolve(_name, _key, {pid1, meta1, _time1}, {pid2, meta2, _time2}) do
    rank1 = Map.fetch!(meta1, :rank)
    rank2 = Map.fetch!(meta2, :rank)

    cond do
      rank1 > rank2 -> pid1
      rank2 > rank1 -> pid2
      pid1 > pid2 -> pid1
      true -> pid2
    end
  end
end
