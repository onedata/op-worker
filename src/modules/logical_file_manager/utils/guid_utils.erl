%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Function for converting paths to guids.
%%% @end
%%%--------------------------------------------------------------------
-module(guid_utils).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([ensure_guid/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Converts given file entry to FileGuid.
%% @end
%%--------------------------------------------------------------------
-spec ensure_guid(session:id(), fslogic_worker:file_guid_or_path()) ->
    {guid, fslogic_worker:file_guid()}.
ensure_guid(_, {guid, FileGuid}) ->
    {guid, FileGuid};
ensure_guid(SessionId, {path, Path}) ->
    remote_utils:call_fslogic(SessionId, fuse_request,
        #resolve_guid{path = Path},
        fun(#guid{guid = Guid}) ->
            {guid, Guid}
        end).

%%%===================================================================
%%% Internal functions
%%%===================================================================