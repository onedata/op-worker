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
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([gen_path/2, gen_storage_path/1]).
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
%% @doc
%% Generate file_meta:path() for given file_meta:entry()
%% @end
%%--------------------------------------------------------------------
-spec gen_path(file_meta:entry(), SessId :: session:id()) ->
    {ok, file_meta:path()} | datastore:generic_error().
gen_path({path, Path}, _SessId) when is_binary(Path) ->
    {ok, Path};
gen_path(Entry, SessId) ->
    ?run(begin
        gen_path(Entry, SessId, [])
    end).

%%--------------------------------------------------------------------
%% @doc
%% Generate storage file_meta:path() for given file_meta:entry()
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_path(file_meta:entry()) ->
    {ok, file_meta:path()} | datastore:generic_error().
gen_storage_path({path, Path}) when is_binary(Path) ->
    {ok, Path};
gen_storage_path(Entry) ->
    ?run(begin
        gen_storage_path(Entry, [])
    end).

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
    #fslogic_ctx{session_id = SessId} = Ctx,
    {ok, #document{value = #onedata_user{space_ids = SpaceIds}}} = onedata_user:get(UserId),

    MatchedSpaceIds = lists:filter(fun(SpaceId) ->
        {ok, #document{value = #space_info{name = Name}}} = space_info:fetch(SpaceId, SessId),
        Name =:= SpaceName
    end, SpaceIds),

    case MatchedSpaceIds of
        [] ->
            throw(?ENOENT);
        [SpaceId] ->
            {path, fslogic_path:join(
                [<<?DIRECTORY_SEPARATOR>>, ?SPACES_BASE_DIR_NAME, SpaceId | Tokens])}
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
-spec basename_and_parent(Path :: file_meta:path()) ->
    {Name :: file_meta:name(), Parent :: file_meta:path()}.
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal helper for gen_path/2. Accumulates all file meta names
%% and concatenates them into path().
%% @end
%%--------------------------------------------------------------------
-spec gen_path(file_meta:entry(), session:id(), [file_meta:name()]) ->
    {ok, file_meta:path()} | datastore:generic_error() | no_return().
gen_path(Entry, SessId, Tokens) ->
    SpaceBaseDirUUID = ?SPACES_BASE_DIR_UUID,
    {ok, #document{key = UUID, value = #file_meta{name = Name}} = Doc} = file_meta:get(Entry),
    case file_meta:get_parent(Doc) of
        {ok, #document{key = ?ROOT_DIR_UUID}} ->
            {ok, fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, Name | Tokens])};
        {ok, #document{key = SpaceBaseDirUUID}} ->
            SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(UUID),
            {ok, #document{value = #space_info{name = SpaceName}}} = space_info:fetch(SpaceId, SessId),
            gen_path({uuid, SpaceBaseDirUUID}, SessId, [SpaceName | Tokens]);
        {ok, #document{key = ParentUUID}} ->
            gen_path({uuid, ParentUUID}, SessId, [Name | Tokens])
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal helper for gen_storage_path/1. Accumulates all file meta names
%% and concatenates them into storage path().
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_path(file_meta:entry(), [file_meta:name()]) ->
    {ok, file_meta:path()} | datastore:generic_error() | no_return().
gen_storage_path(Entry, Tokens) ->
    {ok, #document{value = #file_meta{name = Name}} = Doc} = file_meta:get(Entry),
    case file_meta:get_parent(Doc) of
        {ok, #document{key = ?ROOT_DIR_UUID}} ->
            {ok, fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, Name | Tokens])};
        {ok, #document{key = ParentUUID}} ->
            gen_storage_path({uuid, ParentUUID}, [Name | Tokens])
    end.