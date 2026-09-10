defmodule Group.ReplicaLifecycleModel do
  @moduledoc """
  Independent application-level oracle for replica convergence tests.

  This model deliberately knows nothing about Group's oplog, receive cursors,
  batching, authority tables, or wire frames. It records only operations that
  the public API accepted and the owner lifecycle consequences that follow.
  """

  defstruct owners: %{},
            registrations: %{},
            memberships: MapSet.new(),
            seen_registration_keys: MapSet.new(),
            seen_membership_keys: MapSet.new()

  @type owner_id :: non_neg_integer()
  @type cluster :: term()
  @type key :: binary()
  @type scoped_key :: {cluster(), key()}
  @type owner :: %{node: atom(), alive?: boolean()}
  @type t :: %__MODULE__{
          owners: %{optional(owner_id()) => owner()},
          registrations: %{optional(scoped_key()) => %{optional(owner_id()) => map()}},
          memberships: MapSet.t({cluster(), key(), owner_id(), map()}),
          seen_registration_keys: MapSet.t(scoped_key()),
          seen_membership_keys: MapSet.t(scoped_key())
        }

  def new, do: %__MODULE__{}

  def owner(%__MODULE__{} = model, owner_id), do: Map.get(model.owners, owner_id)

  def put_owner(%__MODULE__{} = model, owner_id, node) do
    case owner(model, owner_id) do
      nil ->
        put_in(model.owners[owner_id], %{node: node, alive?: true})

      %{node: ^node} ->
        model

      %{node: other_node} ->
        raise ArgumentError,
              "logical owner #{inspect(owner_id)} moved from #{inspect(other_node)} to #{inspect(node)}"
    end
  end

  def record_register(%__MODULE__{} = model, owner_id, key, meta, :ok) do
    model
    |> ensure_alive!(owner_id)
    |> Map.update!(:seen_registration_keys, &MapSet.put(&1, key))
    |> Map.update!(:registrations, fn registrations ->
      Map.update(registrations, key, %{owner_id => meta}, &Map.put(&1, owner_id, meta))
    end)
  end

  def record_register(%__MODULE__{} = model, _owner_id, key, _meta, {:error, :taken}) do
    Map.update!(model, :seen_registration_keys, &MapSet.put(&1, key))
  end

  def record_unregister(%__MODULE__{} = model, owner_id, key, :ok) do
    registrations =
      model.registrations
      |> Map.update(key, %{}, &Map.delete(&1, owner_id))
      |> drop_empty_claim_sets()

    %{model | registrations: registrations}
  end

  def record_unregister(%__MODULE__{} = model, _owner_id, _key, {:error, _reason}), do: model

  def record_join(%__MODULE__{} = model, owner_id, {cluster, key} = scope, meta, :ok) do
    model
    |> ensure_alive!(owner_id)
    |> Map.update!(:seen_membership_keys, &MapSet.put(&1, scope))
    |> Map.update!(:memberships, fn memberships ->
      memberships
      |> Enum.reject(fn {member_cluster, member_key, member_owner, _old_meta} ->
        member_cluster == cluster and member_key == key and member_owner == owner_id
      end)
      |> MapSet.new()
      |> MapSet.put({cluster, key, owner_id, meta})
    end)
  end

  def record_leave(%__MODULE__{} = model, owner_id, {cluster, key}, :ok) do
    memberships =
      model.memberships
      |> Enum.reject(fn {member_cluster, member_key, member_owner, _meta} ->
        member_cluster == cluster and member_key == key and member_owner == owner_id
      end)
      |> MapSet.new()

    %{model | memberships: memberships}
  end

  def record_leave(%__MODULE__{} = model, _owner_id, _key, {:error, _reason}), do: model

  def kill(%__MODULE__{} = model, owner_id) do
    case owner(model, owner_id) do
      nil ->
        model

      owner ->
        registrations =
          model.registrations
          |> Map.new(fn {key, claims} -> {key, Map.delete(claims, owner_id)} end)
          |> drop_empty_claim_sets()

        memberships =
          model.memberships
          |> Enum.reject(fn {_cluster, _key, member_owner, _meta} ->
            member_owner == owner_id
          end)
          |> MapSet.new()

        %{
          model
          | owners: Map.put(model.owners, owner_id, %{owner | alive?: false}),
            registrations: registrations,
            memberships: memberships
        }
    end
  end

  def restart_node(%__MODULE__{} = model, node) do
    remove_owned_scope(model, fn owner -> owner.node == node end, fn _scope -> true end)
  end

  def disconnect_cluster(%__MODULE__{} = model, node, cluster) do
    remove_owned_scope(
      model,
      fn owner -> owner.node == node end,
      fn {entry_cluster, _key} -> entry_cluster == cluster end
    )
  end

  def expected_registrations(%__MODULE__{} = model) do
    Map.new(model.registrations, fn {key, claims} ->
      {owner_id, meta} =
        Enum.max_by(claims, fn {owner_id, meta} ->
          {Map.get(meta, :rank, owner_id), owner_id}
        end)

      {key, {owner_id, meta}}
    end)
  end

  def expected_memberships(%__MODULE__{} = model) do
    model.memberships
    |> Enum.group_by(
      fn {cluster, key, _owner_id, _meta} -> {cluster, key} end,
      fn {_cluster, _key, owner_id, meta} -> {owner_id, meta} end
    )
    |> Map.new(fn {key, members} -> {key, Enum.sort(members)} end)
  end

  def resolve_registry_conflicts(%__MODULE__{} = model) do
    losing_owners =
      model.registrations
      |> Enum.flat_map(fn {_key, claims} ->
        if map_size(claims) > 1 do
          {winner, _meta} =
            Enum.max_by(claims, fn {owner_id, meta} ->
              {Map.get(meta, :rank, owner_id), owner_id}
            end)

          Map.keys(claims) -- [winner]
        else
          []
        end
      end)
      |> MapSet.new()

    if MapSet.size(losing_owners) == 0 do
      model
    else
      losing_owners
      |> Enum.reduce(model, &kill(&2, &1))
      |> resolve_registry_conflicts()
    end
  end

  defp ensure_alive!(model, owner_id) do
    case owner(model, owner_id) do
      %{alive?: true} -> model
      other -> raise ArgumentError, "owner #{inspect(owner_id)} is not alive: #{inspect(other)}"
    end
  end

  defp drop_empty_claim_sets(registrations) do
    Map.reject(registrations, fn {_key, claims} -> map_size(claims) == 0 end)
  end

  defp remove_owned_scope(model, owner_filter, scope_filter) do
    owner_ids =
      model.owners
      |> Enum.filter(fn {_owner_id, owner} -> owner_filter.(owner) end)
      |> MapSet.new(&elem(&1, 0))

    registrations =
      model.registrations
      |> Map.new(fn {scope, claims} ->
        claims =
          if scope_filter.(scope) do
            Map.reject(claims, fn {owner_id, _meta} -> MapSet.member?(owner_ids, owner_id) end)
          else
            claims
          end

        {scope, claims}
      end)
      |> drop_empty_claim_sets()

    memberships =
      model.memberships
      |> Enum.reject(fn {cluster, key, owner_id, _meta} ->
        scope_filter.({cluster, key}) and MapSet.member?(owner_ids, owner_id)
      end)
      |> MapSet.new()

    %{model | registrations: registrations, memberships: memberships}
  end
end
