%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
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
-export([gen_path/2, gen_storage_path/1, check_path/1]).
-export([tokenize_skipping_dots/1, get_canonical_file_entry/2]).
-export([split/1, join/1, basename_and_parent/1]).
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
        {ok, UserId} = session:get_user_id(SessId),
        gen_path(Entry, UserId, [])
    end).

%%--------------------------------------------------------------------
%% @doc
%% Checks if path to file is ok.
%% @end
%%--------------------------------------------------------------------
-spec check_path(file_meta:uuid()) -> ok | path_beg_error | {path_error,
    {file_meta:uuid(), file_meta:name(), file_meta:uuid()}}.
check_path(Uuid) ->
    case file_meta:get({uuid, Uuid}) of
        {ok, #document{value = #file_meta{name = Name}} = Doc} ->
            case file_meta:get_parent_uuid(Doc) of
                {ok, ?ROOT_DIR_UUID} ->
                    ok;
                {ok, ParentUuid} ->
                    check_path(ParentUuid, Name, Uuid);
                _ ->
                    path_beg_error
            end;
        _ ->
            path_beg_error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Generate storage file_meta:path() for given file_meta:entry()
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_path(file_meta:entry()) ->
    {ok, file_meta:path()} | datastore:generic_error().
gen_storage_path({path, Path}) when is_binary(Path) ->
    {ok, Uuid} = file_meta:to_uuid({path, Path}),
    SpaceId = fslogic_spaces:get_space_id({uuid, Uuid}),
    {ok, #document{key = StorageId}} = fslogic_storage:select_storage(SpaceId), %todo use file_ctx
    {ok, filename_mapping:to_storage_path(SpaceId, StorageId, Path)};
gen_storage_path(Entry) ->
    ?run(begin
        {ok, Path} = gen_storage_path(Entry, []),
        case file_meta:to_uuid({path, Path}) of %todo delete this case after merging with VFS-2856
            {ok, Uuid} ->
                SpaceId = fslogic_spaces:get_space_id({uuid, Uuid}),
                {ok, #document{key = StorageId}} = fslogic_storage:select_storage(SpaceId), %todo use file_ctx
                {ok, filename_mapping:to_storage_path(SpaceId, StorageId, Path)};
            {error, _ } ->
                {ok, Path}
        end
    end).

%%--------------------------------------------------------------------
%% @doc Gets file's full name.
%% @end
%%--------------------------------------------------------------------
-spec get_canonical_file_entry(user_ctx:ctx(), [file_meta:path()]) ->
    file_meta:entry() | no_return().
get_canonical_file_entry(UserCtx, Tokens) ->
    case session:is_special(user_ctx:get_session_id(UserCtx)) of
        true ->
            {path, fslogic_path:join(Tokens)};
        false ->
            get_canonical_file_entry_for_user(UserCtx, Tokens)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets file's full name, checking user defined space names.
%% @end
%%--------------------------------------------------------------------
-spec get_canonical_file_entry_for_user(user_ctx:ctx(), [file_meta:path()]) -> file_meta:entry() | no_return().
get_canonical_file_entry_for_user(UserCtx, [<<?DIRECTORY_SEPARATOR>>]) ->
    UserId = user_ctx:get_user_id(UserCtx),
    {uuid, fslogic_uuid:user_root_dir_uuid(UserId)};
get_canonical_file_entry_for_user(UserCtx, [<<?DIRECTORY_SEPARATOR>>, SpaceName | Tokens]) ->
    #document{value = #od_user{space_aliases = Spaces}} = user_ctx:get_user(UserCtx),
    case lists:keyfind(SpaceName, 2, Spaces) of
        false ->
            throw(?ENOENT);
        {SpaceId, _} ->
            {path, fslogic_path:join(
                [<<?DIRECTORY_SEPARATOR>>, SpaceId | Tokens])}
    end;
get_canonical_file_entry_for_user(_UserCtx, Tokens) ->
    Path = fslogic_path:join([<<?DIRECTORY_SEPARATOR>> | Tokens]),
    {path, Path}.

%%--------------------------------------------------------------------
%% @doc Strips '.' from path. Also if '..' path element if present, path is considered invalid.
%% @end
%%--------------------------------------------------------------------
-spec tokenize_skipping_dots(FileName :: file_meta:path()) -> Result when
    Result :: {ok, Tokens :: [binary()]} | {error, wrong_filename}.
tokenize_skipping_dots(FileName) ->
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
        _ -> {<<"">>, <<?DIRECTORY_SEPARATOR>>}
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
-spec gen_path(file_meta:entry(), od_user:id(), [file_meta:name()]) ->
    {ok, file_meta:path()} | datastore:generic_error() | no_return().
gen_path(Entry, UserId, Tokens) ->
    {ok, #document{key = UUID, value = #file_meta{name = Name}} = Doc} = file_meta:get(Entry),
    case file_meta:get_parent(Doc) of
        {ok, #document{key = ?ROOT_DIR_UUID}} ->
            SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(UUID),
            {ok, #document{value = #od_space{name = SpaceName}}} =
                od_space:get(SpaceId, UserId),
            {ok, fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, SpaceName | Tokens])};
        {ok, #document{key = ParentUUID}} ->
            gen_path({uuid, ParentUUID}, UserId, [Name | Tokens])
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if path to file is ok.
%% @end
%%--------------------------------------------------------------------
-spec check_path(file_meta:uuid(), file_meta:name(), file_meta:uuid()) ->
    ok | {path_error, {file_meta:uuid(), file_meta:name(), file_meta:uuid()}}.
check_path(Uuid, Name, ChildUuid) ->
    case file_meta:get({uuid, Uuid}) of
        {ok, #document{value = #file_meta{name = NewName}} = Doc} ->
            case file_meta:get_child(Doc, Name) of
                {ok, UUIDs} ->
                    case lists:member(ChildUuid, UUIDs) of
                        true ->
                            case file_meta:get_parent_uuid(Doc) of
                                {ok, ?ROOT_DIR_UUID} ->
                                    ok;
                                {ok, ParentUuid} ->
                                    check_path(ParentUuid, NewName, Uuid);
                                _ ->
                                    {path_error, {Uuid, Name, ChildUuid}}
                            end;
                        false ->
                            {path_error, {Uuid, Name, ChildUuid}}
                    end;
                _ ->
                    {path_error, {Uuid, Name, ChildUuid}}
            end;
        _ ->
            {path_error, {Uuid, Name, ChildUuid}}
    end.