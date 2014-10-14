%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module gives high level DB API which contain administrative group specific methods.
%% @end
%% ===================================================================
-module(dao_groups).


-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include_lib("dao/include/couch_db.hrl").
-include_lib("dao/include/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API - cluster state
-export([save_group/1, get_group/1, remove_group/1]).


%% ===================================================================
%% API functions
%% ===================================================================

%% save_group/1
%% ====================================================================
%% @doc Saves a group to DB. Argument should be either #group_details{} record
%% (if you want to save it as new document) <br/>
%% or #db_document{} that wraps #group_details{} if you want to update the group in DB.
%% @end
-spec save_group(Group :: group_info() | user_doc()) -> {ok, group()} | {error, any()} | no_return().
%% ====================================================================
save_group(#group_details{} = Group) ->
    save_group(#db_document{record = Group});

save_group(#db_document{record = #group_details{}, uuid = UUID} = GroupDoc) when is_binary(UUID), UUID =/= <<"">> ->
    save_group(GroupDoc#db_document{uuid = binary_to_list(UUID)});

save_group(#db_document{record = #group_details{}, uuid = UUID} = GroupDoc) when is_list(UUID), UUID =/= "" ->
    dao_external:set_db(?COOKIES_DB_NAME),
    dao_records:save_record(GroupDoc);

save_group(#db_document{record = #group_details{}} = GroupDoc) ->
    dao_external:set_db(?COOKIES_DB_NAME),
    dao_records:save_record(GroupDoc#db_document{}).


%% get_group/1
%% ====================================================================
%% @doc Gets a session group from DB by its UUID. UUID should be the same as group value.
%% Non-error return value is always {ok, #db_document{record = #group_details}.
%% @end
-spec get_group(UUID :: group() | binary()) -> {ok, group_doc()} | {error, any()} | no_return().
%% ====================================================================
get_group(UUID) when is_binary(UUID) ->
    get_group(binary_to_list(UUID));

get_group(UUID) ->
    dao_external:set_db(?COOKIES_DB_NAME),
    dao_records:get_record(UUID).


%% remove_group/1
%% ====================================================================
%% @doc Gets a session group from DB by its UUID. UUID should be the same as group id value.
%% Non-error return value is always {ok, #db_document{record = #group_details}.
%% @end
-spec remove_group(UUID :: group() | binary()) -> ok | {error, any()} | no_return().
%% ====================================================================
remove_group(UUID) when is_binary(UUID) ->
    remove_group(binary_to_list(UUID));

remove_group(UUID) ->
    dao_external:set_db(?COOKIES_DB_NAME),
    dao_records:remove_record(UUID).

