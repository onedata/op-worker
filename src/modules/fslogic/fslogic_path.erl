%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides set of path processing methods.
%% @end
%% ===================================================================
-module(fslogic_path).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([verify_file_path/1, get_canonical_file_entry/2]).
-export([basename/1, split/1, join/1, is_space_dir/1, basename_and_parent/1]).
-export([dirname/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Returns path of parent directory for given path.
%% @end
%%--------------------------------------------------------------------
-spec dirname(TokensOrPath :: [binary()] | file_meta:path()) -> file_meta:path().
dirname(Tokens) when is_list(Tokens) ->
    fslogic_path:join(lists:sublist(Tokens, 1, length(Tokens) - 1));
dirname(Path) when is_binary(Path) ->
    dirname(split(Path)).

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

%%--------------------------------------------------------------------
%% @doc Gets file's full name (user's root is added to name, but only when
%% asking about non-group dir).
%% @end
%%--------------------------------------------------------------------
-spec get_canonical_file_entry(Ctx :: fslogic_worker:ctx(), Tokens :: [file_meta:path()]) ->
    FileEntry :: file_meta:entry() | no_return().
get_canonical_file_entry(Ctx, [<<?DIRECTORY_SEPARATOR>>]) ->
    UserId = fslogic_context:get_user_id(Ctx),
    {uuid, fslogic_uuid:default_space_uuid(UserId)};
get_canonical_file_entry(Ctx, [<<?DIRECTORY_SEPARATOR>>, ?SPACES_BASE_DIR_NAME]) ->
    UserId = fslogic_context:get_user_id(Ctx),
    Path = fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, UserId, ?SPACES_BASE_DIR_NAME]),
    {path, Path};
get_canonical_file_entry(Ctx, [<<?DIRECTORY_SEPARATOR>>, ?SPACES_BASE_DIR_NAME, SpaceName | Tokens]) ->
    UserId = fslogic_context:get_user_id(Ctx),

    Spaces = case UserId of
        ?ROOT_USER_ID ->
            {ok, Docs} = space_info:list(),
            Docs;
        _ ->
            {ok, #document{value = #onedata_user{space_ids = SpaceIds}}} = onedata_user:get(UserId),
            lists:map(fun(SpaceId) ->
                {ok, Doc} = space_info:get(SpaceId),
                Doc
            end, SpaceIds)
    end,

    Len = size(SpaceName),
    MatchedSpacesIds = lists:filtermap(fun
        (#document{key = Id, value = #space_info{name = Name}}) ->
            CommonPrefixLen = binary:longest_common_prefix([
                SpaceName,
                <<Name/binary, ?SPACE_NAME_ID_SEPARATOR, Id/binary>>
            ]),
            case CommonPrefixLen of
                Len -> {true, Id};
                _ -> false
            end
    end, Spaces),

    case MatchedSpacesIds of
        [] ->
            throw(?ENOENT);
        [SpaceId] ->
            {path, fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, ?SPACES_BASE_DIR_NAME, SpaceId | Tokens])}
    end;
get_canonical_file_entry(Ctx, Tokens) ->
    {ok, DefaultSpaceId} = fslogic_spaces:get_default_space_id(Ctx),
    Path = fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, ?SPACES_BASE_DIR_NAME, DefaultSpaceId | Tokens]),
    {path, Path}.

%%--------------------------------------------------------------------
%% @doc Strips '.' from path. Also if '..' path element if present, path is considered invalid.
%% @end
%%--------------------------------------------------------------------
-spec verify_file_path(FileName :: file_meta:path()) -> Result when
    Result :: {ok, Tokens :: [binary()]} | {error, wrong_filename}.
verify_file_path(FileName) ->
    Tokens = lists:filter(fun(X) -> X =/= <<".">> end, split(FileName)),
    case lists:any(fun(X) -> X =:= <<"..">> end, Tokens) of
        true -> {error, wrong_filename};
        _ -> {ok, Tokens}
    end.

%%--------------------------------------------------------------------
%% @doc Gives file's name based on its path.
%% @end
%%--------------------------------------------------------------------
-spec basename(Path :: file_meta:path()) -> file_meta:path().
basename(Path) ->
    case lists:reverse(split(Path)) of
        [Leaf | _] -> Leaf;
        _ -> <<?DIRECTORY_SEPARATOR>>
    end.

%%--------------------------------------------------------------------
%% @doc Returns file's name and its parent's path.
%% @end
%%--------------------------------------------------------------------
-spec basename_and_parent(Path :: file_meta:path()) -> {Name :: file_meta:name(), Parent :: file_meta:path()}.
basename_and_parent(Path) ->
    case lists:reverse(split(Path)) of
        [Leaf | Tokens] ->
            {Leaf, join([<<?DIRECTORY_SEPARATOR>> | lists:reverse(Tokens)])};
        _ -> {<<"">>, <<?DIRECTORY_SEPARATOR>>}
    end.

%%--------------------------------------------------------------------
%% @doc Returns true when Path points to space directory (or space root directory)
%% @end
%%--------------------------------------------------------------------
-spec is_space_dir(Path :: file_meta:path()) -> boolean().
is_space_dir(Path) ->
    case split(Path) of
        [] -> true;
        [?SPACES_BASE_DIR_NAME] -> true;
        [?SPACES_BASE_DIR_NAME, _SpaceName] -> true;
        _ -> false
    end.