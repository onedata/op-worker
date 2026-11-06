%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for handling lfm:file_key().
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_file_key).
-author("Tomasz Lichon").

-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    resolve_file_key/3,
    ensure_guid/2
]).

-type symlink_resolution_policy() :: resolve_symlink | do_not_resolve_symlink.


%%%===================================================================
%%% API
%%%===================================================================


-spec resolve_file_key(session:id(), lfm:file_key(), symlink_resolution_policy()) ->
    fslogic_worker:file_guid() | no_return().
resolve_file_key(SessionId, FileKey, DefaultSymlinkResolutionPolicy) ->
    {ok, Guid} = ensure_guid(SessionId, FileKey),
    ShouldResolveSymlink = should_resolve_symlink(FileKey, DefaultSymlinkResolutionPolicy),

    case ShouldResolveSymlink andalso fslogic_file_id:is_symlink_guid(Guid) of
        true ->
            Result = remote_utils:call_fslogic(
                SessionId,
                file_request,
                Guid,
                #resolve_symlink{},
                fun(#guid{guid = TargetGuid}) -> TargetGuid end
            ),
            case Result of
                {error, _} = Error -> throw(Error);
                _ -> Result
            end;
        false ->
            Guid
    end.


-spec ensure_guid(session:id(), lfm:file_key()) ->
    {ok, file_id:file_guid()} | {error, term()}.
ensure_guid(SessionId, {path, Path}) ->
    remote_utils:call_fslogic(
        SessionId,
        fuse_request,
        #resolve_guid{path = Path},
        fun(#guid{guid = Guid}) -> {ok, Guid} end
    );
ensure_guid(_, ?FILE_REF(Guid)) ->
    {ok, Guid}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec should_resolve_symlink(lfm:file_key(), symlink_resolution_policy()) ->
    boolean().
should_resolve_symlink({path, _}, resolve_symlink) -> true;
should_resolve_symlink({path, _}, do_not_resolve_symlink) -> false;
should_resolve_symlink(#file_ref{follow_symlink = default}, resolve_symlink) -> true;
should_resolve_symlink(#file_ref{follow_symlink = default}, do_not_resolve_symlink) -> false;
should_resolve_symlink(#file_ref{follow_symlink = Boolean}, _) -> Boolean.
