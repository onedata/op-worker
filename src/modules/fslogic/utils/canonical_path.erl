%% ===================================================================
%% @author Rafal Slota
%% @copyright (C) 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc
%% This module provides set of path processing methods.
%% @end
%% ===================================================================
-module(canonical_path).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([split_skipping_dots/1]).
-export([to_uuid/2]).
-export([resolve/1, resolve/2]).


%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Strips '.' from path. Also if '..' path element if present, path is considered invalid.
%% @end
%%--------------------------------------------------------------------
-spec split_skipping_dots(FileName :: file_meta:path()) -> Result when
    Result :: {ok, Tokens :: [binary()]} | {error, wrong_filename}.
split_skipping_dots(FileName) ->
    Tokens = filepath_utils:split(FileName),
    case lists:any(fun(X) -> X =:= <<"..">> end, Tokens) of
        true -> {error, wrong_filename};
        _ -> {ok, Tokens}
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns UUID of parent's child by name.
%% @end
%%-------------------------------------------------------------------
-spec to_uuid(file_meta:uuid(), file_meta:name()) ->
    {ok, file_meta:uuid()} | {error, term()}.
to_uuid(ParentUuid, Name) ->
    case file_meta:get_child_uuid_and_tree_id(ParentUuid, Name) of
        {ok, Uuid, _} -> {ok, Uuid};
        Error = {error, _} -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Resolves given file_meta:path() and returns file_meta:entry() along with list of
%% all ancestors' UUIDs.
%% @end
%%--------------------------------------------------------------------
-spec resolve(file_meta:path()) ->
    {ok, file_meta:doc()} | {error, term()}.
resolve(Path) ->
    resolve({uuid, ?GLOBAL_ROOT_DIR_UUID}, Path).

-spec resolve(file_meta:entry(), file_meta:path()) ->
    {ok, file_meta:doc()} | {error, term()}.
resolve(Parent, <<?DIRECTORY_SEPARATOR, Path/binary>>) ->
    ?run(begin
        {ok, Root} = file_meta:get(Parent),
        Tokens = filepath_utils:split(Path),
        get_leaf(Root, Tokens)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Returns file meta document associated with path leaf.
%% @end
%%--------------------------------------------------------------------
-spec get_leaf(file_meta:doc(), [file_meta:name()]) ->
    {ok, file_meta:doc()} | {error, term()}.
get_leaf(Doc, []) ->
    {ok, Doc};
get_leaf(#document{key = ParentUuid}, [Name | Names]) ->
    case file_meta:get_child(ParentUuid, Name) of
        {ok, Doc} -> get_leaf(Doc, Names);
        {error, Reason} -> {error, Reason}
    end.
