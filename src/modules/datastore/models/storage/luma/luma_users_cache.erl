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
-module(luma_users_cache).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE,
    memory_copies => all
}).

% TODO invalidation ?

-type id() :: storage:id().
-type record() :: #luma_users_cache{}.
-type diff() :: datastore_doc:diff(record()).
-type storage() :: storage:id() | storage:data().

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get(storage(), od_user:id()) ->
    {ok, luma_user:credentials()} | {error, term()}.
get(Storage, UserId) ->
   case get_internal(Storage, UserId) of
       {ok, StorageUser} ->
           {ok, StorageUser};
       {error, not_found} ->
           acquire_and_cache(Storage, UserId)
   end.


-spec delete(id()) -> ok | {error, term()}.
delete(StorageId) ->
    datastore_model:delete(?CTX, StorageId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_internal(storage(), od_user:id()) ->
    {ok, luma_user:credentials()} | {error, term()}.
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
    {ok, luma_user:credentials()} | {error, term()}.
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
    IsSpaceOwner = fslogic_uuid:is_space_owner(UserId),
    case storage:is_luma_enabled(Storage) andalso not IsSpaceOwner of
        true ->
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
            end;
        false ->
            case {storage:is_posix_compatible(Storage), IsSpaceOwner} of
                {true, false} ->
                    Uid = luma_utils:generate_uid(UserId),
                    {ok, #{<<"uid">> => integer_to_binary(Uid)}, Uid};
                {_, true} ->
                    {ok, SpaceId} = fslogic_uuid:unpack_space_owner(UserId),
                    {ok, SpacePosixCredentials} = luma_spaces_cache:get(Storage, SpaceId),
                    DefaultUid = luma_space:get_default_uid(SpacePosixCredentials),
                    DisplayUid = luma_space:get_display_uid(SpacePosixCredentials),
                    {ok, #{<<"uid">> => integer_to_binary(DefaultUid)}, DisplayUid};
                _ ->
                    Helper = storage:get_helper(Storage),
                    Uid = luma_utils:generate_uid(UserId),
                    {ok, helper:get_admin_ctx(Helper), Uid}
            end
    end.


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


-spec cache(id(), od_user:id(), luma_user:credentials()) -> ok.
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
