%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for luma_storage_users module.
%%% It encapsulates #luma_storage_user{} record.
%%%
%%% This record has 2 fields:
%%%  * storage_credentials - context of user (helpers:user_ctx())
%%%    passed to helper to perform operations on storage as a given user.
%%%  * display_uid - field used to display owner of a file (UID)
%%%    in Oneclient.
%%%
%%% For more info please read the docs of luma.erl and
%%% luma_storage_users.erl modules.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_storage_user).
-author("Jakub Kudzia").

-behaviour(luma_db_record).

-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([new/3, new_posix_user/1, get_storage_credentials/1, get_display_uid/1]).
%% luma_db_record callbacks
-export([to_json/1, from_json/1]).


-record(luma_storage_user, {
    % credentials used to perform operations on behalf of the user on storage
    storage_credentials :: luma:storage_credentials(),
    % optional value for overriding uid returned
    % in #file_attr{} record for get_file_attr operation
    display_uid :: luma:uid()
}).

-type user() ::  #luma_storage_user{}.
-export_type([user/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(od_user:id(), external_luma:storage_user(), storage:id() | storage:data()) ->
    user().
new(UserId, StorageUserMap, Storage) ->
    StorageUserMap2 = ensure_display_uid_defined(UserId, StorageUserMap, Storage),
    #luma_storage_user{
        storage_credentials = maps:get(<<"storageCredentials">>, StorageUserMap2),
        display_uid = maps:get(<<"displayUid">>, StorageUserMap2)
    }.

-spec new_posix_user(luma:uid()) -> user().
new_posix_user(Uid) ->
    #luma_storage_user{
        storage_credentials = #{<<"uid">> => integer_to_binary(Uid)},
        display_uid = Uid
    }.

-spec get_storage_credentials(user()) -> luma:storage_credentials().
get_storage_credentials(#luma_storage_user{storage_credentials = Credentials}) ->
    Credentials.

-spec get_display_uid(user()) -> luma:uid().
get_display_uid(#luma_storage_user{display_uid = DisplayUid}) ->
    DisplayUid.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function ensures that DisplayUid is defined.
%% In case it is undefined:
%% - on POSIX compatible storages use, uid from StorageCredentials
%% - on POSIX incompatible storages generate uid basing on UserId
%% @end
%%--------------------------------------------------------------------
-spec ensure_display_uid_defined(od_user:id(), external_luma:storage_user(), storage:id() | storage:data()) ->
    external_luma:storage_user().
ensure_display_uid_defined(UserId, StorageUserMap, Storage) ->
    case maps:get(<<"displayUid">>, StorageUserMap, undefined) =:= undefined of
        true ->
            DisplayUid = case storage:is_posix_compatible(Storage) of
                true ->
                    StorageCredentials = maps:get(<<"storageCredentials">>, StorageUserMap),
                    binary_to_integer(maps:get(<<"uid">>, StorageCredentials));
                false ->
                    luma_utils:generate_uid(UserId)
            end,
            StorageUserMap#{<<"displayUid">> => DisplayUid};
        false ->
            StorageUserMap
    end.


%%%===================================================================
%%% luma_db_record callbacks
%%%===================================================================

-spec to_json(user()) -> json_utils:json_map().
to_json(#luma_storage_user{
    storage_credentials = StorageCredentials,
    display_uid = DisplayUid
}) ->
    #{
        <<"storage_credentials">> => StorageCredentials,
        <<"display_uid">> => utils:undefined_to_null(DisplayUid)
    }.

-spec from_json(json_utils:json_map()) -> user().
from_json(UserJson) ->
    #luma_storage_user{
        storage_credentials = maps:get(<<"storage_credentials">>, UserJson),
        display_uid = utils:null_to_undefined(maps:get(<<"display_uid">>, UserJson, undefined))
    }.