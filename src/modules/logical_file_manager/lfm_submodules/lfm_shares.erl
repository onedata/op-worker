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
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/oz/oz_shares.hrl").

%% API
-export([create_share/3, remove_share/2, remove_share_by_guid/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a share for given file.
%% @end
%%--------------------------------------------------------------------
-spec create_share(session:id(), logical_file_manager:file_key(), od_share:name()) ->
    {ok, {od_share:id(), od_share:share_guid()}} | logical_file_manager:error_reply().
create_share(SessId, FileKey, Name) ->
    {guid, GUID} = fslogic_uuid:ensure_guid(SessId, FileKey),
    fslogic_utils:call_fslogic(SessId, provider_request, GUID,
        #create_share{name = Name},
        fun(#share{share_id = ShareId, share_file_uuid = ShareGuid}) ->
            {ok, {ShareId, ShareGuid}} end).

%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%% @end
%%--------------------------------------------------------------------
-spec remove_share(session:id(), od_share:id()) ->
    ok | logical_file_manager:error_reply().
remove_share(SessId, ShareID) ->
    case share_logic:get(provider, ShareID) of
        {ok, #document{value = #od_share{root_file = ShareGuid}}} ->
            remove_share_by_guid(SessId, ShareGuid);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareGuid.
%% @end
%%--------------------------------------------------------------------
-spec remove_share_by_guid(session:id(), od_share:share_guid()) ->
    ok | logical_file_manager:error_reply().
remove_share_by_guid(SessId, ShareGuid) ->
    fslogic_utils:call_fslogic(SessId, provider_request, ShareGuid,
        #remove_share{},
        fun(_) -> ok end).