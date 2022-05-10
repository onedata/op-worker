%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for listing recursively non-directory files 
%%% (i.e regular, symlinks and hardlinks) in subtree of given top directory. 
%%% Files are listed in lexicographically ordered by path.
%%% For each such file returns its file basic attributes (see file_attr.hrl) 
%%% along with the path to the file relative to the top directory.
%%% All directory paths user does not have access to are returned under `inaccessible_paths` key.
%%% Available options: 
%%%     * limit - maximum number of entries that will be returned in a single request. 
%%%               For this value both `files` and `inaccessible_paths` are calculated;
%%%     * start_after - determines starting point of listing i.e. listing will start from 
%%%                     the first file path lexicographically larger and listing will continue 
%%%                     until all subtree is listed/limit is reached;
%%%     * prefix - only files with paths that begin with this value will be returned;
%%% @end
%%%--------------------------------------------------------------------
-module(recursive_file_listing).
-author("Michal Stanisz").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    list/3
]).

-type prefix() :: binary().
-type pagination_token() :: binary().
-type limit() :: file_listing:limit().
-type entry() :: {file_meta:path(), lfm_attrs:file_attributes()}.
-type progress_marker() :: more | done.

-record(state, {
    %% values provided in options (unchanged during listing)
    start_after_path :: file_meta:path(),   % required for checking whether file, that is to be 
                                            % appended to result, is not pointed by a start_after_path 
                                            % (listing should start AFTER file with this path).
    prefix = <<>> :: prefix(),
    
    % depth of listing root file relative to space root (unchanged during listing)
    root_file_depth :: non_neg_integer(),
    
    %% values that are modified during listing
    % number of files that are still required to be listed
    limit :: limit(),
    % tokens of start_after_path trimmed to be relative to currently listed file
    relative_start_after_path_tokens :: [file_meta:name()],
    % name of last listed file, when in subtree pointed by start_after_path
    last_start_after_token :: file_meta:name(),
    % absolute path tokens to currently processed directory
    current_dir_path_tokens :: [file_meta:name()],
    % uuid of parent of currently listed file
    parent_uuid :: file_meta:uuid()
}).

-record(list_result, {
    entries = [] :: [entry()],
    inaccessible_paths = [] :: [file_meta:path()]
}).

% For detailed options description see module doc.
-type options() :: #{
    pagination_token => pagination_token(),
    prefix => prefix(),
    limit => limit()
} | #{
    start_after_path => file_meta:path(),
    prefix => prefix(),
    limit => limit()
}.

-type state() :: #state{}.
-type result() :: #list_result{}.

-export_type([prefix/0, pagination_token/0, limit/0, entry/0, options/0]).

-define(LIST_RECURSIVE_BATCH_SIZE, 1000).
-define(DEFAULT_PREFIX, <<>>).
-define(DEFAULT_START_AFTER_PATH, <<>>).

%%%===================================================================
%%% API
%%%===================================================================


-spec list(user_ctx:ctx(), file_ctx:ctx(), options())-> fslogic_worker:fuse_response().
list(_UserCtx, _FileCtx, #{pagination_token := _, start_after := _}) ->
    %% TODO VFS-7208 introduce conflicting options error after introducing API errors to fslogic
    throw(?EINVAL);
list(UserCtx, FileCtx, #{pagination_token := PaginationToken} = Options) ->
    {TokenRootGuid, StartAfter} = unpack_pagination_token(PaginationToken), 
    Limit = maps:get(limit, Options, ?LIST_RECURSIVE_BATCH_SIZE),
    Prefix = maps:get(prefix, Options, ?DEFAULT_PREFIX),
    case file_ctx:get_logical_guid_const(FileCtx) of
        TokenRootGuid -> list(UserCtx, FileCtx, StartAfter, Limit, Prefix);
        _ -> throw(?EINVAL) % requested listing of other dir than token's origin
    end;
list(UserCtx, FileCtx, Options) ->
    StartAfter = maps:get(start_after_path, Options, ?DEFAULT_START_AFTER_PATH),
    Limit = maps:get(limit, Options, ?LIST_RECURSIVE_BATCH_SIZE),
    Prefix = maps:get(prefix, Options, ?DEFAULT_PREFIX),
    list(UserCtx, FileCtx, StartAfter, Limit, Prefix).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list(user_ctx:ctx(), file_ctx:ctx(), file_meta:path(), limit(), prefix())-> 
    fslogic_worker:fuse_response().
list(UserCtx, FileCtx0, StartAfter, Limit, Prefix) ->
    {FilePath, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),
    [_Sep, SpaceId | FilePathTokens] = filename:split(FilePath),
    OptimizedStartAfter = max(StartAfter, Prefix),
    InitialState = #state{
        start_after_path = StartAfter, % use original start after, so when prefix points to existing file it is not ignored
        relative_start_after_path_tokens = filename:split(OptimizedStartAfter),
        limit = Limit,
        current_dir_path_tokens = [SpaceId | FilePathTokens],
        last_start_after_token = filename:basename(FilePath),
        parent_uuid = file_ctx:get_logical_uuid_const(FileCtx1),
        prefix = Prefix,
        root_file_depth = length(FilePathTokens) + 1
    },
    case file_ctx:is_dir(FileCtx1) of
        {true, FileCtx2} ->
            {ProgressMarker, #list_result{entries = Files, inaccessible_paths = InaccessiblePaths}} =
                process_current_dir(UserCtx, FileCtx2, InitialState),
            #fuse_response{status = #status{code = ?OK},
                fuse_response = #recursive_file_list{
                    entries = Files,
                    inaccessible_paths = InaccessiblePaths,
                    pagination_token = build_pagination_token(
                        Files, InaccessiblePaths, file_ctx:get_logical_guid_const(FileCtx1), ProgressMarker)
                }
            };
        {false, FileCtx2} ->
            {Entries, InaccessiblePaths} = case check_reg_file_access(UserCtx, FileCtx2) of
                ok -> 
                    {build_result_file_entry_list(InitialState, get_file_attrs(UserCtx, FileCtx2), <<>>), []};
                {error, ?EACCES} ->
                    {[], <<".">>}
            end,
            #fuse_response{status = #status{code = ?OK},
                fuse_response = #recursive_file_list{
                    inaccessible_paths = InaccessiblePaths,
                    entries = Entries
                }
            }
    end.


%% @private
-spec process_current_dir(user_ctx:ctx(), file_ctx:ctx(), state()) ->
    {progress_marker(), result()}.
process_current_dir(_UserCtx, _FileCtx, #state{limit = Limit}) when Limit =< 0 ->
    {more, #list_result{}};
process_current_dir(UserCtx, FileCtx, State) ->
    Path = build_current_dir_rel_path(State),
    % directories with path that not match given prefix, but is a prefix of this given prefix must be listed, 
    % as they are ancestor directories to files matching this given prefix
    IsAncestorDir = case Path of
        <<".">> -> true; % listing root directory
        _ -> str_utils:binary_starts_with(State#state.prefix, Path)
    end,
    %% @TODO VFS-VFS-9347 When prefix is provided start listing in dir with path pointed by prefix without last token if is a descendant of given dir
    case matches_prefix(Path, State) orelse IsAncestorDir of
        true ->
            {ListOpts, UpdatedState} = init_current_dir_processing(State),
            process_current_dir_in_batches(UserCtx, FileCtx, ListOpts, UpdatedState, #list_result{});
        false ->
            {done, #list_result{}}
    end.


%% @private
-spec process_current_dir_in_batches(user_ctx:ctx(), file_ctx:ctx(), file_listing:options(), state(), result()) -> 
    {progress_marker(), result()}.
process_current_dir_in_batches(UserCtx, FileCtx, ListOpts, State, AccListResult) ->
    #state{limit = Limit} = State,
    {Children, UpdatedAccListResult, ListingPaginationToken, FileCtx2} = 
        case list_dir_children_with_access_check(UserCtx, FileCtx, ListOpts) of
            {ok, C, PT, F} -> 
                {C, AccListResult, PT, F};
            {error, ?EACCES} ->
                PaginationToken = file_listing:infer_pagination_token([], undefined, ?LIST_RECURSIVE_BATCH_SIZE),
                {[], result_append_inaccessible_path(State, AccListResult), PaginationToken, FileCtx}
        end,
    
    {Res, FinalProcessedFileCount} = lists_utils:foldl_while(fun(ChildCtx, {TmpResult, ProcessedFileCount}) ->
        {Marker, SubtreeResult} = process_subtree(UserCtx, ChildCtx, State#state{
            limit = Limit - result_length(TmpResult)
        }),
        ResToReturn = merge_results(TmpResult, SubtreeResult),
        FileProcessedIncrement = case Marker of
            done -> 1;
            more -> 0
        end,
        ToReturn = {ResToReturn, ProcessedFileCount + FileProcessedIncrement},
        case result_length(ResToReturn) >= Limit of
            true -> {halt, ToReturn};
            false -> {cont, ToReturn}
        end
    end, {UpdatedAccListResult, 0}, Children),
    
    ResultLength = result_length(Res),
    case ResultLength > Limit of
        true -> 
            ?critical("Listed more entries than requested in recuresive file listing of directory: ~p~nstate: ~p~noptions: ~p~n", 
                [file_ctx:get_logical_guid_const(FileCtx), State, ListOpts]),
            throw(?ERROR_INTERNAL_SERVER_ERROR);
        _ -> 
            ok
    end, 
    
    case {ResultLength, file_listing:is_finished(ListingPaginationToken)} of
        {Limit, IsFinished} ->
            ProgressMarker = case IsFinished and (FinalProcessedFileCount == length(Children)) of
                true -> done;
                false -> more
            end,
            {ProgressMarker, Res};
        {_, true} ->
            {done, Res};
        {_, false} ->
            NextListOpts = #{pagination_token => ListingPaginationToken},
            process_current_dir_in_batches(
                UserCtx, FileCtx2, NextListOpts, State#state{limit = Limit - result_length(Res)}, Res)
    end.


%% @private
-spec process_subtree(user_ctx:ctx(), file_ctx:ctx(), state()) ->
    {progress_marker(), result()}.
process_subtree(UserCtx, ChildCtx, #state{current_dir_path_tokens = CurrentPathTokens} = State) ->
    case get_file_attrs(UserCtx, ChildCtx) of
        #file_attr{type = ?DIRECTORY_TYPE, name = Name, guid = G} ->
            {ProgressMarker, NextChildrenRes} = process_current_dir(UserCtx, ChildCtx,
                State#state{
                    current_dir_path_tokens = CurrentPathTokens ++ [Name],
                    parent_uuid = file_id:guid_to_uuid(G)
                }
            ),
            {ProgressMarker, NextChildrenRes};
        #file_attr{type = _, name = Name} = FileAttrs ->
            UpdatedState = State#state{current_dir_path_tokens = CurrentPathTokens},
            {done, #list_result{entries = build_result_file_entry_list(UpdatedState, FileAttrs, Name)}}
    end.


%%%===================================================================
%%% Helper functions 
%%%===================================================================

%% @private
-spec list_dir_children_with_access_check(user_ctx:ctx(), file_ctx:ctx(), file_listing:options()) ->
    {ok, [file_ctx:ctx()], file_listing:pagination_token(), file_ctx:ctx()} | {error, ?EACCES}.
list_dir_children_with_access_check(UserCtx, DirCtx, ListOpts) ->
    try
        {CanonicalChildrenWhiteList, DirCtx2} = fslogic_authz:ensure_authorized_readdir(
            UserCtx, DirCtx, [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?list_container_mask)]),
        {Children, ExtendedInfo, DirCtx3} = file_tree:list_children(
            DirCtx2, UserCtx, ListOpts, CanonicalChildrenWhiteList),
        {ok, Children, ExtendedInfo, DirCtx3}
    catch throw:?EACCES ->
        {error, ?EACCES}
    end.


%% @private
-spec check_reg_file_access(user_ctx:ctx(), file_ctx:ctx()) -> ok | {error, ?EACCES}.
check_reg_file_access(UserCtx, FileCtx) ->
    try
        {CanonicalChildrenWhiteList, FileCtx2} = fslogic_authz:ensure_authorized_readdir(
            UserCtx, FileCtx, [?TRAVERSE_ANCESTORS]
        ),
        % throws on lack of access
        _ = file_tree:list_children(FileCtx2, UserCtx, #{
                limit => 1, 
                tune_for_large_continuous_listing => false
            }, CanonicalChildrenWhiteList),
        ok
    catch throw:?EACCES ->
        {error, ?EACCES}
    end.


%% @private
-spec get_file_attrs(user_ctx:ctx(), file_ctx:ctx()) -> lfm_attrs:file_attributes().
get_file_attrs(UserCtx, FileCtx) ->
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = FileAttrs
    } = attr_req:get_file_attr_insecure(UserCtx, FileCtx, #{
        name_conflicts_resolution_policy => resolve_name_conflicts
    }),
    FileAttrs.

    
%% @private
-spec init_current_dir_processing(state()) -> {file_listing:options(), state()}.
init_current_dir_processing(#state{relative_start_after_path_tokens = []} = State) ->
    {#{tune_for_large_continuous_listing => true}, State};
init_current_dir_processing(#state{
    relative_start_after_path_tokens = [CurrentStartAfterToken | NextStartAfterTokens],
    last_start_after_token = LastStartAfterToken,
    current_dir_path_tokens = CurrentPathTokens,
    parent_uuid = ParentUuid
} = State) ->
    InitialOpts = #{limit => ?LIST_RECURSIVE_BATCH_SIZE, tune_for_large_continuous_listing => true},
    %% As long as the currently processed path is within the start_after_path, we can start
    %% listing from the specific file name (CurrentStartAfterToken), as all names lexicographically
    %% smaller should not be included in the results. Otherwise, The whole directory is listed and processed.
    case lists:last(CurrentPathTokens) == LastStartAfterToken of
        true ->
            ListingOpts = case file_meta:get_child_uuid_and_tree_id(ParentUuid, CurrentStartAfterToken) of
                {ok, _, TreeId} ->
                    StartingIndex = file_listing:build_index(
                        file_meta:trim_filename_tree_id(CurrentStartAfterToken, TreeId), TreeId),
                    #{index => StartingIndex, inclusive => true};
                _ ->
                    StartingIndex = file_listing:build_index(file_meta:trim_filename_tree_id(
                        CurrentStartAfterToken, {all, ParentUuid})),
                    #{index => StartingIndex}
            end,
            UpdatedState = State#state{
                relative_start_after_path_tokens = NextStartAfterTokens, 
                last_start_after_token = CurrentStartAfterToken
            },
            {maps:merge(InitialOpts, ListingOpts), UpdatedState};
        _ ->
            {InitialOpts, State#state{relative_start_after_path_tokens = []}}
    end.


%% @private
-spec build_result_file_entry_list(state(), lfm_attrs:file_attributes(), file_meta:name()) -> 
    [entry()].
build_result_file_entry_list(#state{start_after_path = StartAfterPath} = State, FileAttrs, Name) ->
    case build_rel_path_in_current_dir_with_prefix_check(State, Name) of
        false -> [];
        {true, StartAfterPath} -> [];
        {true, Path} -> [{Path, FileAttrs}]
    end.


%% @private
-spec build_current_dir_rel_path_with_prefix_check(state()) -> {true, file_meta:path()} | false.
build_current_dir_rel_path_with_prefix_check(State) ->
    build_rel_path_in_current_dir_with_prefix_check(State, <<>>).


%% @private
-spec build_rel_path_in_current_dir_with_prefix_check(state(), file_meta:name()) -> 
    {true, file_meta:path()} | false.
build_rel_path_in_current_dir_with_prefix_check(State, Name) ->
    Path = build_rel_path_in_current_dir(State, Name),
    case matches_prefix(Path, State) of
        true -> {true, Path};
        false -> false
    end.


%% @private
-spec matches_prefix(file_meta:path(), state()) -> boolean().
matches_prefix(Path, #state{prefix = Prefix}) ->
    str_utils:binary_starts_with(Path, Prefix).


%% @private
-spec build_current_dir_rel_path(state()) -> file_meta:path().
build_current_dir_rel_path(State) ->
    build_rel_path_in_current_dir(State, <<>>).


%% @private
-spec build_rel_path_in_current_dir(state(), file_meta:name()) -> file_meta:path().
build_rel_path_in_current_dir(#state{current_dir_path_tokens = CurrentPathTokens, root_file_depth = Depth}, Name) ->
    case filename:join(lists:sublist(CurrentPathTokens ++ [Name], Depth + 1, length(CurrentPathTokens))) of
        <<>> -> <<".">>;
        Path -> Path
    end.


%% @private
-spec build_pagination_token([entry()], [file_meta:path()], file_id:file_guid(), progress_marker()) ->
    pagination_token() | undefined.
build_pagination_token(_, _, _, _ProgressMarker = done) -> undefined;
build_pagination_token([], [], _, _) -> undefined;
build_pagination_token(Files, InaccessiblePaths, RootGuid, _ProgressMarker = more) ->
    StartAfter = case {InaccessiblePaths, Files} of
        {[], _} ->
            {T, _} = lists:last(Files),
            T;
        {_, []} ->
            lists:last(InaccessiblePaths);
        {_, _} ->
            {T, _} = lists:last(Files),
            max(T, lists:last(InaccessiblePaths))
    end,
    pack_pagination_token(RootGuid, StartAfter).


-spec pack_pagination_token(file_id:file_guid(), file_meta:path()) -> pagination_token().
pack_pagination_token(RootGuid, StartAfter) ->
    mochiweb_base64url:encode(json_utils:encode(#{<<"guid">> => RootGuid, <<"startAfter">> => StartAfter})).


-spec unpack_pagination_token(pagination_token()) -> {file_id:file_guid(), file_meta:path()} | no_return().
unpack_pagination_token(Token) ->
    try json_utils:decode(mochiweb_base64url:decode(Token)) of
        #{<<"guid">> := RootGuid, <<"startAfter">> := StartAfter} ->
            {RootGuid, StartAfter};
        _ ->
            throw({error, ?EINVAL})
    catch _:_ ->
        throw({error, ?EINVAL})
    end.

%%%===================================================================
%%% Helper functions operating on #list_result record
%%%===================================================================

%% @private
-spec result_append_inaccessible_path(state(), result()) -> result().
result_append_inaccessible_path(State, Result) ->
    StartAfterPath = State#state.start_after_path,
    case build_current_dir_rel_path_with_prefix_check(State) of
        false -> Result;
        {true, StartAfterPath} -> Result;
        {true, Path} -> merge_results(Result, #list_result{inaccessible_paths = [Path]})
    end.


%% @private
-spec result_length(result()) -> non_neg_integer().
result_length(#list_result{inaccessible_paths = IP, entries = F}) ->
    length(IP) + length(F).


%% @private
-spec merge_results(result(), result()) -> result().
merge_results(#list_result{entries = F1, inaccessible_paths = IP1}, #list_result{entries = F2, inaccessible_paths = IP2}) ->
    #list_result{
        entries = F1 ++ F2,
        inaccessible_paths = IP1 ++ IP2
    }.
