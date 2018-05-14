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
-module(fslogic_path).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([split_skipping_dots/1]).
-export([split/1, join/1, basename_and_parent/1, logical_to_canonical_path/2,
    to_uuid/2]).
-export([resolve/1, resolve/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Same as {@link filename:split/1} but platform independent.
%% @end
%%--------------------------------------------------------------------
-spec split(Path :: file_meta:path()) -> [binary()].
split(Path) ->
    Bins = binary:split(Path, <<?DIRECTORY_SEPARATOR>>, [global]),
    Bins1 = [Bin || Bin <- Bins, Bin =/= <<"">>],
    case Path of
        <<?DIRECTORY_SEPARATOR, _/binary>> ->
            [<<?DIRECTORY_SEPARATOR>> | Bins1];
        _ -> Bins1
    end.

%%--------------------------------------------------------------------
%% @doc Joins binary tokens into a binary path.
%% @end
%%--------------------------------------------------------------------
-spec join(list(binary())) -> binary().
join([]) ->
    <<>>;
join([<<?DIRECTORY_SEPARATOR>> = Sep, <<?DIRECTORY_SEPARATOR>> | Tokens]) ->
    join([Sep | Tokens]);
join([<<?DIRECTORY_SEPARATOR>> = Sep | Tokens]) ->
    <<Sep/binary, (join(Tokens))/binary>>;
join(Tokens) ->
    Tokens1 = lists:map(fun
        StripFun(Token) ->
            Size1 = byte_size(Token) - 1,
            Size2 = byte_size(Token) - 2,
            case Token of
                <<?DIRECTORY_SEPARATOR, Tok:(Size2)/binary, ?DIRECTORY_SEPARATOR>> ->
                    StripFun(Tok);
                <<?DIRECTORY_SEPARATOR, Tok/binary>> ->
                    StripFun(Tok);
                <<Tok:Size1/binary, ?DIRECTORY_SEPARATOR>> ->
                    StripFun(Tok);
                Tok ->
                    Tok
            end
    end, Tokens),
    Tokens2 = [Bin || Bin <- Tokens1, Bin =/= <<"">>],
    binary_join(Tokens2, <<?DIRECTORY_SEPARATOR>>).

%%--------------------------------------------------------------------
%% @doc Strips '.' from path. Also if '..' path element if present, path is considered invalid.
%% @end
%%--------------------------------------------------------------------
-spec split_skipping_dots(FileName :: file_meta:path()) -> Result when
    Result :: {ok, Tokens :: [binary()]} | {error, wrong_filename}.
split_skipping_dots(FileName) ->
    Tokens = lists:filter(fun(X) -> X =/= <<".">> end, split(FileName)),
    case lists:any(fun(X) -> X =:= <<"..">> end, Tokens) of
        true -> {error, wrong_filename};
        _ -> {ok, Tokens}
    end.

%%--------------------------------------------------------------------
%% @doc Returns file's name and its parent's path.
%% @end
%%--------------------------------------------------------------------
-spec basename_and_parent(Path :: file_meta:path()) ->
    {Name :: file_meta:name(), Parent :: file_meta:path()}.
basename_and_parent(Path) ->
    case lists:reverse(split(Path)) of
        [Leaf | Tokens] ->
            {Leaf, join([<<?DIRECTORY_SEPARATOR>> | lists:reverse(Tokens)])};
        _ ->
            {<<"">>, <<?DIRECTORY_SEPARATOR>>}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Converts logical to canonical path.
%% @end
%%--------------------------------------------------------------------
-spec logical_to_canonical_path(file_meta:path(), od_space:id()) -> file_meta:path().
logical_to_canonical_path(LogicalPath, SpaceId) ->
    [<<"/">>, _SpaceName | Rest] = split(LogicalPath),
    join([<<"/">>, SpaceId | Rest]).

%%-------------------------------------------------------------------
%% @doc
%% Returns UUID of parent's child by name.
%% @end
%%-------------------------------------------------------------------
-spec to_uuid(file_meta:uuid(), file_meta:name()) ->
    {ok, file_meta:uuid()} | {error, term()}.
to_uuid(ParentUuid, Name) ->
    file_meta:get_child_uuid(ParentUuid, Name).

%%--------------------------------------------------------------------
%% @doc
%% Resolves given file_meta:path() and returns file_meta:entry() along with list of
%% all ancestors' UUIDs.
%% @end
%%--------------------------------------------------------------------
-spec resolve(file_meta:path()) ->
    {ok, file_meta:doc()} | {error, term()}.
resolve(Path) ->
    resolve({uuid, ?ROOT_DIR_UUID}, Path).

-spec resolve(file_meta:entry(), file_meta:path()) ->
    {ok, file_meta:doc()} | {error, term()}.
resolve(Parent, <<?DIRECTORY_SEPARATOR, Path/binary>>) ->
    ?run(begin
        {ok, Root} = file_meta:get(Parent),
        Tokens = fslogic_path:split(Path),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Join binaries separating them with Separator
%% @end
%%--------------------------------------------------------------------
-spec binary_join([binary()], binary()) -> binary().
binary_join([], _Sep) ->
    <<>>;
binary_join([Part], _Sep) ->
    Part;
binary_join(List, Sep) ->
    lists:foldr(fun(A, B) ->
        if
            bit_size(B) > 0 -> <<A/binary, Sep/binary, B/binary>>;
            true -> A
        end
    end, <<>>, List).