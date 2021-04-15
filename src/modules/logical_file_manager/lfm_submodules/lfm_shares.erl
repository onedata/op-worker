%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module performs shares-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_shares).
-author("Lukasz Opiola").

-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([create_share/4, remove_share/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_share(session:id(), lfm:file_key(), od_share:name(), od_share:description()) ->
    {ok, od_share:id()} | lfm:error_reply().
create_share(SessId, FileKey, Name, Description) ->
    remote_utils:call_fslogic(
        SessId,
        provider_request,
        lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
        #create_share{name = Name, description = Description},
        fun(#share{share_id = ShareId}) -> {ok, ShareId} end
    ).


-spec remove_share(session:id(), od_share:id()) -> ok | lfm:error_reply().
remove_share(SessId, ShareId) ->
    case share_logic:get(SessId, ShareId) of
        {ok, #document{value = #od_share{root_file = ShareGuid}}} ->
            remote_utils:call_fslogic(
                SessId,
                provider_request,
                file_id:share_guid_to_guid(ShareGuid),
                #remove_share{share_id = ShareId},
                fun(_) -> ok end
            );
        Error ->
            Error
    end.
