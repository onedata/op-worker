%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(luma_spaces_cache).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([get/2, delete/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE,
    memory_copies => all
}).

-type id() :: storage:id().
-type record() :: #luma_spaces_cache{}.
-type diff() :: datastore_doc:diff(record()).
-type storage() :: storage:id() | storage:data().

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get(storage(), od_space:id()) -> {ok, luma_space:posix_credentials()} | {error, term()}.
get(Storage, SpaceId) ->
    case get_internal(Storage, SpaceId) of
        {ok, SupportCredentials} ->
            {ok, SupportCredentials};
        {error, not_found} ->
            acquire_and_cache(Storage, SpaceId)
    end.

-spec delete(id()) -> ok | {error, term()}.
delete(StorageId) ->
    datastore_model:delete(?CTX, StorageId).

-spec delete(storage:id(), od_space:id()) -> ok.
delete(StorageId, SpaceId) ->
    Diff = fun(LS = #luma_spaces_cache{spaces = Spaces}) ->
        {ok, LS#luma_spaces_cache{spaces = maps:remove(SpaceId, Spaces)}}
    end,
    case datastore_model:update(?CTX, StorageId, Diff) of
        {error, not_found} ->
            ok;
        {ok, #document{value = #luma_spaces_cache{spaces = Spaces}}} when map_size(Spaces) =:= 0 ->
            Pred = fun(#luma_spaces_cache{spaces = Spaces2}) -> map_size(Spaces2) =:= 0 end,
            case datastore_model:delete(?CTX, StorageId, Pred) of
                ok -> ok;
                {error, {not_satisfied, _}} -> ok
            end;
        {ok, _} ->
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_internal(storage(), od_space:id()) ->
    {ok, luma_space:posix_credentials()} | {error, term()}.
get_internal(Storage, SpaceId) ->
    StorageId = storage:get_id(Storage),
    case datastore_model:get(?CTX, StorageId) of
        {ok, #document{value = #luma_spaces_cache{spaces = Spaces}}} ->
            case maps:get(SpaceId, Spaces, undefined) of
                undefined -> {error, not_found};
                PosixCredentials -> {ok, PosixCredentials}
            end;
        Error ->
            Error
    end.


-spec acquire_and_cache(storage(), od_space:id()) ->
    {ok, luma_space:posix_credentials()} | {error, term()}.
acquire_and_cache(Storage, SpaceId) ->
    % ensure Storage is a document
    {ok, StorageData} = storage:get(Storage),
    try
        {ok, PosixCredentials} = acquire(StorageData, SpaceId),
        cache(storage:get_id(StorageData), SpaceId, PosixCredentials),
        {ok, PosixCredentials}
    catch
        throw:Reason ->
            {error, Reason}
    end.


-spec acquire(storage:data(), od_space:id()) -> 
    {ok, luma_space:posix_credentials()}.
acquire(Storage, SpaceId) ->
    IgnoreLumaDefaultOwner = storage:is_imported_storage(Storage)
        andalso storage:is_posix_compatible(Storage),
    {DefOwner2, DisplayOwner2} = case {storage:is_luma_enabled(Storage), IgnoreLumaDefaultOwner} of
        {true, false} ->
            {ok, DefOwner} = fetch_default_owner(Storage, SpaceId),
            {ok, DisplayOwner} = fetch_display_override_owner(Storage, SpaceId),
            {DefOwner, DisplayOwner};
        {true, true} ->
            {ok, DisplayOwner} = fetch_display_override_owner(Storage, SpaceId),
            {#{}, DisplayOwner};
        {false, _} ->
            {#{}, #{}}
    end,
    {ok, refill_with_defaults(DefOwner2, DisplayOwner2, SpaceId, Storage)}.


-spec fetch_default_owner(storage:data(), od_space:id()) -> 
    {ok, luma:space_entry()} | {error, term()}.
fetch_default_owner(Storage, SpaceId) ->
    case external_luma:fetch_default_owner(SpaceId, Storage) of
        {ok, DefaultOwner} -> {ok, DefaultOwner};
        {error, not_found} -> {ok, #{}};
        {error, Reason} -> throw(Reason)
    end.

-spec fetch_display_override_owner(storage:data(), od_space:id()) ->
    {ok, luma:space_entry()} | {error, term()}.
fetch_display_override_owner(Storage, SpaceId) ->
    case external_luma:fetch_display_override_owner(SpaceId, Storage) of
        {ok, DisplayOwner} -> {ok, DisplayOwner};
        {error, not_found} -> {ok, #{}};
        {error, Reason} -> throw(Reason)
    end.

-spec refill_with_defaults(luma:space_entry(), luma:space_entry(),
    od_space:id(), storage:data()) -> luma_space:posix_credentials().
refill_with_defaults(DefaultOwner, DisplayOwner, SpaceId, Storage) ->
    DefaultUid = maps:get(<<"uid">>, DefaultOwner, undefined),
    DefaultGid = maps:get(<<"gid">>, DefaultOwner, undefined),
    DisplayUid = maps:get(<<"uid">>, DisplayOwner, undefined),
    DisplayGid = maps:get(<<"gid">>, DisplayOwner, undefined),

    {DefaultUid2, DefaultGid2} = case {DefaultUid, DefaultGid} of
        {undefined, undefined} ->
            get_posix_compatible_fallback_owner(Storage, SpaceId);
        {_, undefined} ->
            {_, FallbackGid} = get_posix_compatible_fallback_owner(Storage, SpaceId),
            {DefaultUid, FallbackGid};
        {undefined, _} ->
            {FallbackUid, _} = get_posix_compatible_fallback_owner(Storage, SpaceId),
            {FallbackUid, DefaultGid};
        _ ->
            {DefaultUid, DefaultGid}
    end,

    DisplayUid2 = case DisplayUid =:= undefined of
        true -> DefaultUid2;
        false -> DisplayUid
    end,

    DisplayGid2 = case DisplayGid =:= undefined of
        true -> DefaultGid2;
        false -> DisplayGid
    end,
    luma_space:new(DefaultUid2, DefaultGid2, DisplayUid2, DisplayGid2).

-spec cache(storage:id(), od_space:id(), luma_space:posix_credentials()) -> ok.
cache(StorageId, SpaceId, PosixCredentials) ->
    update(StorageId, fun(SSC = #luma_spaces_cache{spaces = Spaces}) ->
        {ok, SSC#luma_spaces_cache{
            spaces = Spaces#{SpaceId => PosixCredentials}
        }}
    end).

-spec update(storage:id(), diff()) -> ok.
update(StorageId, Diff) ->
    {ok, Default} = Diff(#luma_spaces_cache{}),
    ok = ?extract_ok(datastore_model:update(?CTX, StorageId, Diff, Default)).

-spec get_posix_compatible_fallback_owner(storage:data(), od_space:id()) -> {luma:uid(), luma:gid()}.
get_posix_compatible_fallback_owner(Storage, SpaceId) ->
    case storage:is_posix_compatible(Storage) of
        true ->
            get_mountpoint_owner(Storage, SpaceId);
        false ->
            Uid = luma_utils:generate_uid(?SPACE_OWNER_ID(SpaceId)),
            Gid = luma_utils:generate_gid(SpaceId),
            {Uid, Gid}
    end.

-spec get_mountpoint_owner(storage:data(), od_space:id()) -> {luma:uid(), luma:gid()}.
get_mountpoint_owner(Storage, SpaceId) ->
    StorageFileCtx = storage_file_ctx:new(?DIRECTORY_SEPARATOR_BINARY, SpaceId, storage:get_id(Storage)),
    {#statbuf{st_uid = Uid, st_gid = Gid}, _} = storage_file_ctx:stat(StorageFileCtx),
    {Uid, Gid}.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {spaces, #{string => {record, [
            {default_uid, integer},
            {default_gid, integer},
            {display_uid, integer},
            {display_gid, integer}
        ]}}}
    ]}.