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

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([create_share/3, remove_share/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a share for given file.
%% @end
%%--------------------------------------------------------------------
-spec create_share(session:id(), fslogic_worker:file_guid_or_path(), od_share:name()) ->
    {ok, {od_share:id(), od_share:root_file_guid()}} | lfm:error_reply().
create_share(SessId, FileKey, Name) ->
    {guid, GUID} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, GUID,
        #create_share{name = Name},
        fun(#share{share_id = ShareId, root_file_guid = ShareGuid}) ->
            {ok, {ShareId, ShareGuid}}
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%% @end
%%--------------------------------------------------------------------
-spec remove_share(session:id(), od_share:id()) ->
    ok | lfm:error_reply().
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
