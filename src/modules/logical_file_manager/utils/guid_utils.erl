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
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    resolve_file_key/3,
    ensure_guid/2,
    resolve_if_symlink/2
]).

%%%===================================================================
%%% API
%%%===================================================================


-spec resolve_file_key(session:id(), lfm:file_key(), lfm:symlink_resolution_policy()) ->
    fslogic_worker:file_guid() | no_return().
resolve_file_key(SessionId, {KeyType, _} = FileKey, DefaultSymlinkResolutionPolicy) ->
    {ok, Guid} = ensure_guid(SessionId, FileKey),
    ShouldResolveSymlink = should_resolve_symlink(KeyType, DefaultSymlinkResolutionPolicy),

    case ShouldResolveSymlink of
        true ->
            {ok, TargetGuid} = resolve_if_symlink(SessionId, Guid),
            TargetGuid;
        false ->
            Guid
    end.


-spec ensure_guid(session:id(), lfm:file_key()) ->
    {ok, file_id:file_guid()} | {error, term()}.
ensure_guid(SessionId, {Type, Path}) when
    Type =:= path;
    Type =:= direct_path;
    Type =:= indirect_path
->
    remote_utils:call_fslogic(
        SessionId,
        fuse_request,
        #resolve_guid{path = Path},
        fun(#guid{guid = Guid}) -> {ok, Guid} end
    );
ensure_guid(_, {_, Guid}) ->
    {ok, Guid}.


-spec resolve_if_symlink(session:id(), file_id:file_guid()) ->
    {ok, file_id:file_guid()} | {error, term()}.
resolve_if_symlink(SessionId, FileGuid) ->
    case fslogic_uuid:is_symlink_guid(FileGuid) of
        true ->
            remote_utils:call_fslogic(
                SessionId,
                file_request,
                FileGuid,
                #resolve_symlink{},
                fun(#guid{guid = TargetGuid}) -> {ok, TargetGuid} end
            );
        false ->
            {ok, FileGuid}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec should_resolve_symlink(lfm:file_key_type(), lfm:symlink_resolution_policy()) ->
    boolean().
should_resolve_symlink(path, resolve_symlink) -> true;
should_resolve_symlink(direct_path, _) -> false;
should_resolve_symlink(indirect_path, _) -> true;

should_resolve_symlink(guid, resolve_symlink) -> true;
should_resolve_symlink(direct_guid, _) -> false;
should_resolve_symlink(indirect_guid, _) -> true;

should_resolve_symlink(_, _) -> false.
