%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for removing files recursively
%%% @end
%%%--------------------------------------------------------------------
-module(remove_utils).
-author("Tomasz Lichon").

-include("global_definitions.hrl").

%% API
-export([rm/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes an object with all its children.
%% @end
%%--------------------------------------------------------------------
-spec rm(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    ok | logical_file_manager:error_reply().
rm(SessId, FileKey) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case is_dir(SessId, Guid) of
        true ->
            case rm_children(SessId, Guid, 0, Chunk, ok) of
                ok ->
                    lfm_files:unlink(SessId, {guid, Guid}, false);
                Error ->
                    Error
            end;
        false ->
            lfm_files:unlink(SessId, {guid, Guid}, false)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if a file is directory.
%% @end
%%--------------------------------------------------------------------
-spec is_dir(session:id(), Guid :: fslogic_worker:file_guid()) ->
    true | false | logical_file_manager:error_reply().
is_dir(SessId, Guid) ->
    case lfm_attrs:stat(SessId, {guid, Guid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} -> true;
        {ok, _} -> false;
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes all children of directory with given UUID.
%% @end
%%--------------------------------------------------------------------
-spec rm_children(session:id(), Guid :: fslogic_worker:file_guid(),
    Offset :: non_neg_integer(), Chunk :: non_neg_integer(), ok | {error, term()}) ->
    ok | logical_file_manager:error_reply().
rm_children(SessId, Guid, Offset, Chunk, Answer) ->
    case lfm_dirs:ls(SessId, {guid, Guid}, Offset, Chunk) of
        {ok, Children} ->
            Answers = lists:map(fun
                ({ChildGuid, _ChildName}) ->
                    rm(SessId, {guid, ChildGuid})
            end, Children),
            {FirstError, ErrorCount} = lists:foldl(fun
                (ok, {Ans, ErrorCount}) -> {Ans, ErrorCount};
                (Error, {ok, ErrorCount}) -> {Error, ErrorCount + 1};
                (_Error, {OldError, ErrorCount}) -> {OldError, ErrorCount + 1}
            end, {Answer, 0}, Answers),

            case length(Children) of
                Chunk ->
                    rm_children(SessId, Guid, ErrorCount, Chunk, FirstError);
                _ -> % no more children
                    FirstError
            end;
        Error ->
            case Answer of
                ok -> Error;
                Other -> Other
            end
    end.