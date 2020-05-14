%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is used for storing LUMA mappings for users.
%%% Mappings are used to associate
%%% onedata users with specific storage users.
%%% Documents of this model are stored per StorageId.
%%% Each documents consists of map #{od_user:id() => luma_user:entry()},
%%% so the mappings are actually associated with pair (storage:id(), od_user:id()).
%%%
%%% For more info in luma_user:entry() structure please see
%%% luma_user.erl module.
%%%
%%% Mappings may be set in 3 ways:
%%%  * filled by default algorithm in case NO_LUMA mode is set for given
%%%    storage - if storage is POSIX compatible, UID is generated and
%%%    used in both fields: storage_credentials and display_uid.
%%%    On POSIX incompatible storages, helper's AdminCtx is used as
%%%    storage_credentials and display_uid is generated.
%%%    (see acquire_default_mapping function).
%%%  * preconfigured using REST API in case EMBEDDED_LUMA
%%%    is set for given storage
%%%  * cached after querying external, 3rd party LUMA server in case
%%%    EXTERNAL_LUMA mode is set for given storage
%%%
%%% For more info please read the docs of luma.erl.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_users_cache).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get/2, delete/1, cache_posix_compatible_mapping/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE,
    memory_copies => all
}).

-type id() :: storage:id().
-type record() :: #luma_users_cache{}.
-type diff() :: datastore_doc:diff(record()).
-type storage() :: storage:id() | storage:data().

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get(storage(), od_user:id()) ->
    {ok, luma_user:entry()} | {error, term()}.
get(Storage, UserId) ->
    case get_internal(Storage, UserId) of
        {ok, StorageUser} ->
            {ok, StorageUser};
        {error, not_found} ->
            acquire_and_cache(Storage, UserId)
    end.

-spec cache_posix_compatible_mapping(storage:data(), od_user:id(), luma:uid()) -> ok.
cache_posix_compatible_mapping(Storage, UserId, Uid) ->
    StorageCredentials = #{<<"uid">> => integer_to_binary(Uid)},
    LumaUserCredentials = luma_user:new(StorageCredentials, Uid),
    cache(storage:get_id(Storage), UserId, LumaUserCredentials).

-spec delete(id()) -> ok | {error, term()}.
delete(StorageId) ->
    datastore_model:delete(?CTX, StorageId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_internal(storage(), od_user:id()) ->
    {ok, luma_user:entry()} | {error, term()}.
get_internal(Storage, UserId) ->
    StorageId = storage:get_id(Storage),
    case datastore_model:get(?CTX, StorageId) of
        {ok, #document{value = #luma_users_cache{users = Users}}} ->
            case maps:get(UserId, Users, undefined) of
                undefined -> {error, not_found};
                StorageUser -> {ok, StorageUser}
            end;
        Error = {error, not_found} ->
            Error
    end.


-spec acquire_and_cache(storage(), od_user:id()) ->
    {ok, luma_user:entry()} | {error, term()}.
acquire_and_cache(Storage, UserId) ->
    % ensure Storage is a document
    {ok, StorageData} = storage:get(Storage),
    case acquire(StorageData, UserId) of
        {ok, StorageCredentials, DisplayUid} ->
            LumaUserCredentials = luma_user:new(StorageCredentials, DisplayUid),
            cache(storage:get_id(StorageData), UserId, LumaUserCredentials),
            {ok, LumaUserCredentials};
        Error ->
            Error
    end.


-spec acquire(storage:data(), od_user:id()) ->
    {ok, luma:storage_credentials(), luma:uid()} | {error, term()}.
acquire(Storage, ?ROOT_USER_ID) ->
    Helper = storage:get_helper(Storage),
    {ok, helper:get_admin_ctx(Helper), ?ROOT_UID};
acquire(Storage, UserId) ->
    case storage:is_luma_enabled(Storage) of
        true -> acquire_mapping_from_external_luma(Storage, UserId);
        false -> acquire_default_mapping(Storage, UserId)
    end.

-spec acquire_mapping_from_external_luma(storage:data(), od_user:id()) ->
    {ok, luma:storage_credentials(), luma:uid()} | {error, term()}.
acquire_mapping_from_external_luma(Storage, UserId) ->
    case external_luma:map_onedata_user_to_credentials(UserId, Storage) of
        {ok, LumaResponse} ->
            StorageCredentials = maps:get(<<"storageCredentials">>, LumaResponse),
            DisplayUid = maps:get(<<"displayUid">>, LumaResponse, undefined),
            DisplayUid2 = ensure_display_uid_defined(DisplayUid, StorageCredentials, UserId, Storage),
            {ok, StorageCredentials, DisplayUid2};
        {error, external_luma_error} ->
            {error, not_found};
        OtherError ->
            OtherError
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function returns default mapping for user in case of NO_LUMA mode.
%% On POSIX compatible storages it generates uid basing on UserId.
%% On POSIX incompatible storages it returns AdminCtx of the storage.
%% @end
%%--------------------------------------------------------------------
-spec acquire_default_mapping(storage:data(), od_user:id()) ->
    {ok, luma:storage_credentials(), luma:uid()} | {error, term()}.
acquire_default_mapping(Storage, UserId) ->
    Uid = luma_utils:generate_uid(UserId),
    case storage:is_posix_compatible(Storage) of
        true ->
            {ok, #{<<"uid">> => integer_to_binary(Uid)}, Uid};
        false ->
            Helper = storage:get_helper(Storage),
            {ok, helper:get_admin_ctx(Helper), Uid}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function ensures that DisplayUid is defined.
%% In case it is undefined:
%% - on POSIX compatible storages use, uid from StorageCredentials
%% - on POSIX incompatible storages generate uid basing on UserId
%% @end
%%--------------------------------------------------------------------
-spec ensure_display_uid_defined(luma:uid() | undefined, luma:storage_credentials(),
    od_user:id(), storage:data()) -> luma:uid().
ensure_display_uid_defined(undefined, StorageCredentials, UserId, Storage) ->
    case storage:is_posix_compatible(Storage) of
        true ->
            binary_to_integer(maps:get(<<"uid">>, StorageCredentials));
        false ->
            luma_utils:generate_uid(UserId)
    end;
ensure_display_uid_defined(DisplayUid, _StorageCredentials, _UserId, _Storage) ->
    DisplayUid.


-spec cache(id(), od_user:id(), luma_user:entry()) -> ok.
cache(StorageId, UserId, LumaUserCredentials) ->
    update(StorageId, fun(LumaUsers = #luma_users_cache{users = Users}) ->
        {ok, LumaUsers#luma_users_cache{
            users = Users#{UserId => LumaUserCredentials}
        }}
    end).


-spec update(id(), diff()) -> ok.
update(StorageId, Diff) ->
    {ok, Default} = Diff(#luma_users_cache{}),
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
        {users, #{string => {record, [
            {storage_credentials, #{string => string}},
            {display_uid, integer}
        ]}}}
    ]}.
