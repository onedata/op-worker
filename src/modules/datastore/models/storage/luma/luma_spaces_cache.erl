%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is used for storing defaults (for spaces) that are used in
%%% LUMA mappings for users.
%%% Documents of this model are stored per StorageId.
%%%
%%% Mappings may be set in 3 ways:
%%%  * filled by default algorithm in case NO_LUMA mode is set for given
%%%    storage
%%%  * preconfigured using REST API in case EMBEDDED_LUMA
%%%    is set for given storage
%%%  * cached after querying external, 3rd party LUMA server in case
%%%    EXTERNAL_LUMA mode is set for given storage
%%%
%%% For more info please read the docs of luma.erl module.
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

-spec get(storage(), od_space:id()) -> {ok, luma_space:entry()} | {error, term()}.
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
    {ok, luma_space:entry()} | {error, term()}.
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
    {ok, luma_space:entry()} | {error, term()}.
acquire_and_cache(Storage, SpaceId) ->
    % ensure Storage is a document
    {ok, StorageData} = storage:get(Storage),
    try
        {ok, LumaSpace} = acquire(StorageData, SpaceId),
        cache(storage:get_id(StorageData), SpaceId, LumaSpace),
        {ok, LumaSpace}
    catch
        throw:Reason ->
            {error, Reason}
    end.


-spec acquire(storage:data(), od_space:id()) -> {ok, luma_space:entry()}.
acquire(Storage, SpaceId) ->
    IsNotPosix = not storage:is_posix_compatible(Storage),
    % default owner is ignored on:
    % - posix incompatible storages
    % - synced storage
    IgnoreLumaDefaultOwner = IsNotPosix orelse storage:is_imported_storage(Storage),
    {DefaultPosixCredentials, DisplayCredentials} = case {storage:is_luma_enabled(Storage), IgnoreLumaDefaultOwner} of
        {true, false} ->
            {ok, DefOwner} = fetch_default_posix_credentials(Storage, SpaceId),
            {ok, DisplayOwner} = fetch_display_credentials(Storage, SpaceId),
            {DefOwner, DisplayOwner};
        {true, true} ->
            {ok, DisplayOwner} = fetch_display_credentials(Storage, SpaceId),
            {#{}, DisplayOwner};
        {false, _} ->
            {#{}, #{}}
    end,
    {ok, luma_space:new(DefaultPosixCredentials, DisplayCredentials, SpaceId, Storage, IgnoreLumaDefaultOwner)}.


-spec fetch_default_posix_credentials(storage:data(), od_space:id()) ->
    {ok, luma:space_mapping_response()} | {error, term()}.
fetch_default_posix_credentials(Storage, SpaceId) ->
    case external_luma:fetch_default_posix_credentials(SpaceId, Storage) of
        {ok, DefaultCredentials} -> {ok, DefaultCredentials};
        {error, not_found} -> {ok, #{}};
        {error, Reason} -> throw(Reason)
    end.

-spec fetch_display_credentials(storage:data(), od_space:id()) ->
    {ok, luma:space_mapping_response()} | {error, term()}.
fetch_display_credentials(Storage, SpaceId) ->
    case external_luma:fetch_default_display_credentials(SpaceId, Storage) of
        {ok, DisplayCredentials} -> {ok, DisplayCredentials};
        {error, not_found} -> {ok, #{}};
        {error, Reason} -> throw(Reason)
    end.

-spec cache(storage:id(), od_space:id(), luma_space:entry()) -> ok.
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