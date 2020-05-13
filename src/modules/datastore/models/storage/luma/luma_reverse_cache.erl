%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is used for storing reverse LUMA mappings.
%%% Mappings are used by storage_sync mechanism to associate
%%% storage users/groups with specific users/groups in onedata.
%%% Documents of this model are stored per StorageId.
%%%
%%% Mappings may be set in 2 ways:
%%%  * preconfigured using REST API in case EMBEDDED_LUMA
%%%    is set for given storage.
%%%  * cached after querying external, 3rd party LUMA server in case
%%%    EXTERNAL_LUMA mode is set for given storage.
%%%
%%% For more info please read the docs of luma.erl module.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_reverse_cache).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([
    map_uid_to_onedata_user/2,
    map_acl_user_to_onedata_user/2,
    map_acl_group_to_onedata_group/2,
    cache_uid_mapping/3,
    delete/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE,
    memory_copies => all
}).

-type id() :: storage:id().
-type record() :: #luma_reverse_cache{}.
-type diff() :: datastore_doc:diff(record()).

-type internal_key() :: luma:uid() | luma:acl_who().
-type internal_value() :: od_user:id() | od_group:id().
-type internal_map() :: #{internal_key() => internal_value()}.

-define(UID, uid).
-define(ACL_USER, acl_user).
-define(ACL_GROUP, acl_group).

-type mode() :: ?UID | ?ACL_USER | ?ACL_GROUP.

%%%===================================================================
%%% API functions
%%%===================================================================

-spec map_uid_to_onedata_user(storage:data(), luma:uid()) ->
    {ok, od_user:id()} | {error, term()}.
map_uid_to_onedata_user(Storage, Uid) ->
   map(Storage, Uid, ?UID).

-spec map_acl_user_to_onedata_user(storage:data(), luma:acl_who()) ->
    {ok, od_user:id()} | {error, term()}.
map_acl_user_to_onedata_user(Storage, AclUser) ->
    map(Storage, AclUser, ?ACL_USER).

-spec map_acl_group_to_onedata_group(storage:data(), luma:acl_who()) ->
    {ok, od_group:id()} | {error, term()}.
map_acl_group_to_onedata_group(Storage, AclGroup) ->
    map(Storage, AclGroup, ?ACL_GROUP).

-spec cache_uid_mapping(storage:data(), luma:uid(), od_user:id()) -> ok.
cache_uid_mapping(Storage, Uid, UserId) ->
    cache(storage:get_id(Storage), Uid, UserId, ?UID).

-spec delete(id()) -> ok | {error, term()}.
delete(StorageId) ->
    datastore_model:delete(?CTX, StorageId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec map(storage:data(), internal_key(), mode()) ->
    {ok, internal_value()} | {error, term()}.
map(Storage, Key, Mode) ->
    case get_internal(Storage, Key, Mode) of
        {ok, Value} ->
            {ok, Value};
        {error, not_found} ->
            acquire_and_cache(Storage, Key, Mode)
    end.

-spec get_internal(storage:data(), internal_key(), mode()) ->
    {ok, internal_value()} | {error, term()}.
get_internal(Storage, Key, Mode) ->
    StorageId = storage:get_id(Storage),
    case datastore_model:get(?CTX, StorageId) of
        {ok, #document{value = LRC}} ->
            case Mode of
                ?UID -> get_from_internal_map(Key, LRC#luma_reverse_cache.users);
                ?ACL_USER -> get_from_internal_map(Key, LRC#luma_reverse_cache.acl_users);
                ?ACL_GROUP -> get_from_internal_map(Key, LRC#luma_reverse_cache.acl_groups)
            end;
        Error ->
            Error
    end.

-spec get_from_internal_map(internal_key(), internal_map()) ->
    {ok, internal_value()} | {error, not_found}.
get_from_internal_map(Key, Map) ->
    case maps:get(Key, Map, undefined) of
        undefined -> {error, not_found};
        Value -> {ok, Value}
    end.

-spec acquire_and_cache(storage:data(), internal_key(), mode()) ->
    {ok, internal_value()} | {error, term()}.
acquire_and_cache(Storage, Key, Mode) ->
    case acquire(Storage, Key, Mode) of
        {ok, Value} ->
            cache(storage:get_id(Storage), Key, Value, Mode),
            {ok, Value};
        Error ->
            Error
    end.

-spec acquire(storage:data(), internal_key(), mode()) ->
    {ok, internal_value()} | {error, term()}.
acquire(Storage, Uid, ?UID) ->
    case external_reverse_luma:map_uid_to_onedata_user(Uid, Storage) of
        {ok, UserId} ->
            % cache reverse mapping
            luma_users_cache:cache_posix_compatible_mapping(Storage, UserId, Uid),
            {ok, UserId};
        Error ->
            Error
    end;
acquire(Storage, AclUser, ?ACL_USER) ->
    external_reverse_luma:map_acl_user_to_onedata_user(AclUser, Storage);
acquire(Storage, AclGroup, ?ACL_GROUP) ->
    external_reverse_luma:map_acl_group_to_onedata_group(AclGroup, Storage).


-spec cache(storage:id(), internal_key(), internal_value(), mode()) -> ok.
cache(StorageId, Uid, UserId, ?UID) ->
    update(StorageId, cache_uid_fun(Uid, UserId));
cache(StorageId, AclUser, UserId, ?ACL_USER) ->
    update(StorageId, cache_acl_user_fun(AclUser, UserId));
cache(StorageId, AclGroup, GroupId, ?ACL_GROUP) ->
    update(StorageId, cache_acl_group_fun(AclGroup, GroupId)).

-spec cache_uid_fun(luma:uid(), od_user:id()) -> diff().
cache_uid_fun(Uid, UserId) ->
    fun(RLC = #luma_reverse_cache{users = Users}) ->
        {ok, RLC#luma_reverse_cache{users = Users#{Uid => UserId}}}
    end.

-spec cache_acl_user_fun(luma:acl_who(), od_user:id()) -> diff().
cache_acl_user_fun(AclUser, UserId) ->
    fun(RLC = #luma_reverse_cache{acl_users = AclUsers}) ->
        {ok, RLC#luma_reverse_cache{acl_users = AclUsers#{AclUser => UserId}}}
    end.

-spec cache_acl_group_fun(luma:acl_who(), od_group:id()) -> diff().
cache_acl_group_fun(AclGroup, GroupId) ->
    fun(RLC = #luma_reverse_cache{acl_groups = AclGroups}) ->
        {ok, RLC#luma_reverse_cache{acl_groups = AclGroups#{AclGroup => GroupId}}}
    end.

-spec update(storage:id(), diff()) -> ok.
update(StorageId, Diff) ->
    {ok, Default} = Diff(#luma_reverse_cache{}),
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
        {users, #{integer => string}},
        {acl_users, #{string => string}},
        {acl_groups, #{string => string}}
    ]}.