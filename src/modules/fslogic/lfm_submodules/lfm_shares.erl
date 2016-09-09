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
-include("proto/oneprovider/provider_messages.hrl").

-type share_id() :: fslogic_worker:file_guid().

-export_type([share_id/0]).

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
-spec create_share(session:id(), logical_file_manager:file_key(), share_info:name()) ->
    {ok, ShareID :: share_id()} | logical_file_manager:error_reply().
create_share(SessId, FileKey, Name) ->
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, provider_request, GUID,
        #create_share{name = Name},
        fun(#share{uuid = ShareGuid}) -> {ok, ShareGuid} end).

%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%% @end
%%--------------------------------------------------------------------
-spec remove_share(session:id(), share_id()) ->
    ok | logical_file_manager:error_reply().
remove_share(SessId, ShareID) ->
    lfm_utils:call_fslogic(SessId, provider_request, ShareID,
        #remove_share{},
        fun(_) -> ok end).
