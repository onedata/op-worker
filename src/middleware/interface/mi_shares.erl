%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing shares (requests are delegated to middleware_worker).
%%% @end
%%%-------------------------------------------------------------------
-module(mi_shares).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    create/4,
    remove/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(session:id(), lfm:file_key(), od_share:name(), od_share:description()) ->
    od_share:id() | no_return().
create(SessionId, FileKey, Name, Description) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),

    middleware_worker:check_exec(SessionId, FileGuid, #create_share{
        name = Name,
        description = Description
    }).


-spec remove(session:id(), od_share:id()) -> ok | no_return().
remove(SessionId, ShareId) ->
    RootFileGuid = get_share_root_file_guid(SessionId, ShareId),

    middleware_worker:check_exec(SessionId, RootFileGuid, #remove_share{share_id = ShareId}).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_share_root_file_guid(session:id(), od_share:id()) ->
    file_id:file_guid() | no_return().
get_share_root_file_guid(SessionId, ShareId) ->
    #document{value = #od_share{root_file = ShareGuid}} = ?check(share_logic:get(
        SessionId, ShareId
    )),
    file_id:share_guid_to_guid(ShareGuid).
