%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that stores list of storage IDs attached to the space.
%%% @end
%%%-------------------------------------------------------------------
-module(space_storage).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([add/2, add/3]).
-export([get_storage_ids/1, get_mounted_in_root/1, is_cleanup_enabled/1]).
-export([save/1, get/1, exists/1, delete/1, update/2, create/1]).

%% datastore_model callbacks
-export([get_posthooks/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type id() :: od_space:id().
-type record() :: #space_storage{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([id/0, record/0, doc/0, diff/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves space storage.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates space storage.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates space storage.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, id()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Returns space storage.
%% @end
%%--------------------------------------------------------------------
-spec get(undefined | id()) -> {ok, doc()} | {error, term()}.
get(undefined) ->
    {error, not_found};
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes space storage.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether space storage exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(id()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds storage to the space.
%% @end
%%--------------------------------------------------------------------
-spec add(od_space:id(), storage:id()) ->
    {ok, od_space:id()} | {error, Reason :: term()}.
add(SpaceId, StorageId) ->
    add(SpaceId, StorageId, false).


%%--------------------------------------------------------------------
%% @doc
%% Adds storage to the space.
%% @end
%%--------------------------------------------------------------------
-spec add(od_space:id(), storage:id(), boolean()) ->
    {ok, od_space:id()} | {error, Reason :: term()}.
add(SpaceId, StorageId, MountInRoot) ->
    Diff = fun(#space_storage{
        storage_ids = StorageIds,
        mounted_in_root = MountedInRoot
    } = Model) ->
        case lists:member(StorageId, StorageIds) of
            true -> {error, already_exists};
            false ->
                SpaceStorage = Model#space_storage{
                    storage_ids = [StorageId | StorageIds]
                },
                case MountInRoot of
                    true ->
                        {ok, SpaceStorage#space_storage{
                            mounted_in_root = [StorageId | MountedInRoot]
                        }};
                    _ ->
                        {ok, SpaceStorage}
                end
        end
    end,
    #document{value = Default} = new(SpaceId, StorageId, MountInRoot),

    case datastore_model:update(?CTX, SpaceId, Diff, Default) of
        {ok, _} ->
            ok = space_strategies:add_storage(SpaceId, StorageId, MountInRoot),
            {ok, SpaceId};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_ids(record() | doc()) -> [storage:id()].
get_storage_ids(#space_storage{storage_ids = StorageIds}) ->
    StorageIds;
get_storage_ids(#document{value = #space_storage{} = Value}) ->
    get_storage_ids(Value).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space that have been mounted in
%% storage root.
%% @end
%%--------------------------------------------------------------------
-spec get_mounted_in_root(record() | doc()) -> [storage:id()].
get_mounted_in_root(#space_storage{mounted_in_root = StorageIds}) ->
    StorageIds;
get_mounted_in_root(#document{value = #space_storage{} = Value}) ->
    get_mounted_in_root(Value).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space that have been mounted in
%% storage root.
%% @end
%%--------------------------------------------------------------------
-spec is_cleanup_enabled(od_space:id()) -> boolean().
is_cleanup_enabled(SpaceId) ->
    case space_storage:get(SpaceId) of
        {ok, Doc} -> Doc#document.value#space_storage.cleanup_enabled;
        {error, not_found} -> false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Space storage create/update posthook.
%% @end
%%--------------------------------------------------------------------
-spec run_after(atom(), list(), term()) -> term().
run_after(create, _, {ok, #document{key = SpaceId}}) ->
    space_cleanup:initialize(SpaceId),
    {ok, SpaceId};
run_after(update, [_, _, _, _], {ok, #document{key = SpaceId}}) ->
    space_cleanup:initialize(SpaceId),
    {ok, SpaceId};
run_after(save, _, {ok, #document{key = SpaceId}}) ->
    space_cleanup:initialize(SpaceId),
    {ok, SpaceId};
run_after(_Function, _Args, Result) ->
    Result.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns space_storage document.
%% @end
%%--------------------------------------------------------------------
-spec(new(od_space:id(), storage:id(), boolean()) -> doc()).
new(SpaceId, StorageId, true) ->
    #document{key = SpaceId, value = #space_storage{
        storage_ids = [StorageId],
        mounted_in_root = [StorageId]}};
new(SpaceId, StorageId, _) ->
    #document{key = SpaceId, value = #space_storage{storage_ids = [StorageId]}}.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun run_after/3].

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    3.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {storage_ids, [string]}
    ]};
get_record_struct(2) ->
    {record, [
        {storage_ids, [string]},
        {mounted_in_root, [string]}
    ]};
get_record_struct(3) ->
    {record, [
        {storage_ids, [string]},
        {mounted_in_root, [string]},
        {cleanup_enabled, boolean}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, StorageIds}) ->
    {2, #space_storage{storage_ids = StorageIds}};
upgrade_record(2, {?MODULE, StorageIds, MountedInRoot}) ->
    {3, #space_storage{storage_ids = StorageIds, mounted_in_root = MountedInRoot}}.


