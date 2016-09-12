%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs shares-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_shares).

-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/oz/oz_shares.hrl").
-include("proto/oneprovider/provider_messages.hrl").

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
-spec create_share(session:id(), logical_file_manager:file_key(), share_info:name()) ->
    {ok, {share_info:id(), share_info:share_guid()}} | logical_file_manager:error_reply().
create_share(SessId, FileKey, Name) ->
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, provider_request, GUID,
        #create_share{name = Name},
        fun(#share{share_id = ShareId, share_file_uuid = ShareGuid}) -> {ok, {ShareId, ShareGuid}} end).

%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%% @end
%%--------------------------------------------------------------------
-spec remove_share(session:id(), share_info:id()) ->
    ok | logical_file_manager:error_reply().
remove_share(SessId, ShareID) ->
    case oz_shares:get_details(provider, ShareID) of
        {ok, #share_details{root_file_id = ShareGuid}} ->
            remove_share_by_guid(SessId, ShareGuid);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareGuid.
%% @end
%%--------------------------------------------------------------------
-spec remove_share_by_guid(session:id(), share_info:share_guid()) ->
    ok | logical_file_manager:error_reply().
remove_share_by_guid(SessId, ShareGuid) ->
    lfm_utils:call_fslogic(SessId, provider_request, ShareGuid,
        #remove_share{},
        fun(_) -> ok end).