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
%%% For each such file returns its file basic attributes (see file_attr.hrl) 
%%% along with relative path to the given top directory.
%%% All directory paths user does not have access to are returned under `inaccessible_paths` key.
%%% Available options: 
%%%     * limit - maximum number of entries that will be returned in a single request. 
%%%               For this value both `files` and `inaccessible_paths` are calculated;
%%%     * start_after - determines starting point of listing i.e. listing will start from 
%%%                     the first file lexicographically larger and listing will continue 
%%%                     until all subtree is listed/limit is reached;
%%%     * prefix - only files with paths that match this value will be returned;
%%% @end
%%%--------------------------------------------------------------------
-module(recursive_file_listing).
-author("Michal Stanisz").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    list/3,
    pack_options/3
]).

-type prefix() :: binary().
-type token() :: binary().
-type limit() :: file_meta:size().
-type result_file_entry() :: {file_meta:path(), lfm_attrs:file_attributes()}.
-type empty_binary() :: binary(). % binary with value exactly <<>>

-record(state, {
    start_after_path :: file_meta:path(),
    start_after_path_tokens :: [file_meta:name()],
    limit :: non_neg_integer(),
    current_path_tokens :: [file_meta:name()],
    prev_start_after_token :: undefined | file_meta:name(),
    parent_uuid :: file_meta:uuid(),
    root_file_depth :: non_neg_integer(),
    prefix = <<>> :: binary()
}).

-record(list_result, {
    files = [] :: [result_file_entry()],
    inaccessible_paths = [] :: [file_meta:path()]
}).

-record(options, {
    start_after :: file_meta:path() | empty_binary(),
    prefix :: prefix() | empty_binary(),
    limit :: limit(),
    guid = undefined :: file_id:file_guid() | undefined
}).

-type state() :: #state{}.
-opaque options() :: #options{}.
-type result() :: #list_result{}.

-export_type([prefix/0, token/0, limit/0, result_file_entry/0, options/0]).

-define(LIST_RECURSIVE_BATCH_SIZE, 1000).

%%%===================================================================
%%% API
%%%===================================================================

-spec list(user_ctx:ctx(), file_ctx:ctx(), options())-> fslogic_worker:fuse_response().
list(UserCtx, FileCtx0, #options{start_after = StartAfter, limit = Limit, prefix = Prefix, guid = undefined}) ->
    {FilePath, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),
    [_Sep, SpaceId | FilePathTokens] = filename:split(FilePath),
    StartAfterPathTokens = filename:split(StartAfter),
    {#list_result{files = Files, inaccessible_paths = InaccessiblePaths}, IsLast} = 
        list_next_file(UserCtx, FileCtx1, #state{
            start_after_path = StartAfter,
            start_after_path_tokens = StartAfterPathTokens,
            limit = Limit,
            current_path_tokens = [SpaceId | FilePathTokens],
            prev_start_after_token = filename:basename(FilePath),
            parent_uuid = file_ctx:get_logical_uuid_const(FileCtx1),
            prefix = Prefix,
            root_file_depth = length(FilePathTokens) + 1
        }),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #recursive_file_list{
            files = Files,
            inaccessible_paths = InaccessiblePaths,
            continuation_token = build_continuation_token(
                Files, InaccessiblePaths, file_ctx:get_logical_guid_const(FileCtx1), IsLast)
        }
    };
list(UserCtx, FileCtx, #options{guid = Guid} = Options) ->
    case file_ctx:get_logical_guid_const(FileCtx) of
        Guid ->
            list(UserCtx, FileCtx, Options#options{guid = undefined});
        _ ->
            throw(?EINVAL)
    end.


-spec pack_options({token, token()} | {start_after, file_meta:path() | undefined}, limit(), 
    prefix() | undefined) -> options().
pack_options({token, Token}, Limit, Prefix) ->
    try json_utils:decode(base64url:decode(Token)) of
        #{<<"guid">> := Guid, <<"startAfter">> := StartAfter} ->
            #options{
                start_after = normalize_list_option(StartAfter),
                limit = Limit,
                prefix = normalize_list_option(Prefix),
                guid = Guid
            };
        _ ->
            throw({error, ?EINVAL})
    catch _:_ ->
        throw({error, ?EINVAL})
    end;
pack_options({start_after, StartAfter}, Limit, Prefix) ->
    #options{
        start_after = normalize_list_option(StartAfter),
        limit = Limit,
        prefix = normalize_list_option(Prefix)
    }.

%%%===================================================================
%%% Listing internal functions
%%%===================================================================

%% @private
-spec list_next_file(user_ctx:ctx(), file_ctx:ctx(), state()) ->
    {result(), boolean()}.
list_next_file(_UserCtx, _FileCtx, #state{limit = Limit}) when Limit =< 0 ->
    {#list_result{}, false};
list_next_file(UserCtx, FileCtx, State) ->
    {ListOpts, UpdatedState} = build_initial_listing_opts(State),
    Path = build_path(State),
    case {is_in_scope(Path, State), State#state.prefix > Path} of
        {false, false} ->
            {#list_result{}, true};
        _ ->
            list_children_next_batch(UserCtx, FileCtx, ListOpts, UpdatedState, #list_result{})
    end.


%% @private
-spec list_children_next_batch(user_ctx:ctx(), file_ctx:ctx(), file_meta:list_opts(), state(), result()) -> 
    {result(), boolean()}.
list_children_next_batch(UserCtx, FileCtx, ListOpts, State, AccListResult) ->
    #state{limit = Limit} = State,
    {Children, UpdatedAccListResult, #{is_last := IsLast} = ExtendedInfo} = 
        case list_children_with_access_check(UserCtx, FileCtx, ListOpts) of
            {C, E, _} -> 
                {C, AccListResult, E};
            {error, ?EACCES} ->
                {[], result_append_inaccessible_path(State, AccListResult), #{is_last => true}}
        end,
    {Res, FinalProcessedFiles} = lists_utils:foldl_while(fun(ChildCtx, {TmpResult, ProcessedFiles}) ->
        {ChildBatch, IsChildProcessed} = map_listed_child(
            UserCtx, ChildCtx, file_ctx:get_logical_guid_const(FileCtx), State#state{
                limit = Limit - result_length(TmpResult)
            }),
        ResToReturn = merge_results(TmpResult, ChildBatch),
        FileProcessedIncrement = case IsChildProcessed of
            true -> 1;
            false -> 0
        end,
        ToReturn = {ResToReturn, ProcessedFiles + FileProcessedIncrement},
        case result_length(ResToReturn) >= Limit of
            true -> {halt, ToReturn};
            false -> {cont, ToReturn}
        end
    end, {UpdatedAccListResult, 0}, Children),
    case {result_length(Res) >= Limit, IsLast} of
        {true, _} ->
            {Res, IsLast and (FinalProcessedFiles == length(Children))};
        {false, true} ->
            {Res, true};
        {false, false} ->
            NextListOpts = maps:with([last_tree, last_name], ExtendedInfo),
            list_children_next_batch(
                UserCtx, FileCtx, NextListOpts, State#state{limit = Limit - result_length(Res)}, Res)
    end.


%% @private
-spec list_children_with_access_check(user_ctx:ctx(), file_ctx:ctx(), file_meta:list_opts()) ->
    {[file_ctx:ctx()], file_meta:list_extended_info(), file_ctx:ctx()} | {error, ?EACCES}.
list_children_with_access_check(UserCtx, FileCtx, ListOpts) ->
    try
        {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
        AccessRequirements = case IsDir of
            true -> [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?list_container_mask)];
            false -> [?TRAVERSE_ANCESTORS]
        end,
        {CanonicalChildrenWhiteList, FileCtx3} = fslogic_authz:ensure_authorized_readdir(
            UserCtx, FileCtx2, AccessRequirements
        ),
        file_listing:list_children(UserCtx, FileCtx3, ListOpts, CanonicalChildrenWhiteList)
    catch throw:?EACCES ->
        {error, ?EACCES}
    end.


%% @private
-spec map_listed_child(user_ctx:ctx(), file_ctx:ctx(), file_id:file_guid(), state()) ->
    {[{file_meta:path(), lfm_attrs:file_attributes()}], boolean()}.
map_listed_child(UserCtx, ChildCtx, ListedFileGuid, State) ->
    #state{
        current_path_tokens = CurrentPathTokens,
        prev_start_after_token = PrevStartAfterToken
    } = State,
    case get_file_attrs(UserCtx, ChildCtx) of
        #file_attr{type = ?DIRECTORY_TYPE, name = Name, guid = G} ->
            {NextChildrenRes, SubtreeFinished} = list_next_file(UserCtx, ChildCtx,
                State#state{
                    current_path_tokens = CurrentPathTokens ++ [Name],
                    parent_uuid = file_id:guid_to_uuid(G)
                }
            ),
            {NextChildrenRes, SubtreeFinished};
        #file_attr{type = _, name = PrevStartAfterToken} ->
            % file pointed by start_after should be ignored in result
            {#list_result{}, true};
        #file_attr{guid = G} = FileAttrs when G == ListedFileGuid ->
            % listing regular file should return this file
            {#list_result{files = build_result_file_entry_list(State, FileAttrs)}, true};
        #file_attr{type = _, name = Name} = FileAttrs ->
            UpdatedState = State#state{current_path_tokens = CurrentPathTokens ++ [Name]},
            {#list_result{files = build_result_file_entry_list(UpdatedState, FileAttrs)}, true}
    end.


%%%===================================================================
%%% Helper functions 
%%%===================================================================

%% @private
-spec normalize_list_option(undefined | T) -> empty_binary() | T.
normalize_list_option(undefined) -> <<>>;
normalize_list_option(Other) -> Other.


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
-spec build_initial_listing_opts(state()) -> {file_meta:list_opts(), state()}.
build_initial_listing_opts(#state{prev_start_after_token = undefined} = State) ->
    {#{last_name => <<>>}, State#state{start_after_path_tokens = []}};
build_initial_listing_opts(#state{start_after_path_tokens = []} = State) ->
    {#{last_name => <<>>}, State#state{prev_start_after_token = undefined}};
build_initial_listing_opts(#state{
    start_after_path_tokens = [CurrentStartAfterToken | NextStartAfterTokens],
    prev_start_after_token = PrevStartAfterToken,
    current_path_tokens = CurrentPathTokens,
    parent_uuid = ParentUuid
} = State) ->
    InitialOpts = #{size => ?LIST_RECURSIVE_BATCH_SIZE}, 
    case lists:last(CurrentPathTokens) == PrevStartAfterToken of
        true ->
            Opts = case file_meta:get_child_uuid_and_tree_id(ParentUuid, CurrentStartAfterToken) of
                {ok, _, TreeId} ->
                    #{
                        % trim tree id to always have inclusive listing
                        last_tree => binary:part(TreeId, 0, size(TreeId) - 1),
                        last_name => file_meta:trim_filename_tree_id(CurrentStartAfterToken, TreeId)
                    };
                _ ->
                    #{
                        last_name => file_meta:trim_filename_tree_id(
                            CurrentStartAfterToken, {all, ParentUuid})
                    }
            end,
            {
                maps:merge(InitialOpts, Opts),
                State#state{
                    start_after_path_tokens = NextStartAfterTokens, 
                    prev_start_after_token = CurrentStartAfterToken
                }
            };
        _ ->
            % we are no longer in a subtree pointed by start_after_path
            {
                InitialOpts#{last_name => <<>>}, 
                State#state{start_after_path_tokens = [], prev_start_after_token = undefined}
            }
    end.


%% @private
-spec build_result_file_entry_list(state(), lfm_attrs:file_attributes()) -> [result_file_entry()].
build_result_file_entry_list(State, FileAttrs) ->
    case build_path_in_requested_scope(State) of
        out_of_scope -> [];
        Path -> [{Path, FileAttrs}]
    end.


%% @private
-spec build_path_in_requested_scope(state()) -> file_meta:path() | out_of_scope.
build_path_in_requested_scope(State) ->
    Path = build_path(State),
    case is_in_scope(Path, State) of
        true ->
            case Path of
                <<>> -> <<".">>;
                Path -> Path
            end;
        false ->
            out_of_scope
    end.


%% @private
-spec is_in_scope(file_meta:path(), state()) -> boolean().
is_in_scope(Path, #state{prefix = Prefix}) ->
    str_utils:binary_starts_with(Path, Prefix).


%% @private
-spec build_path(state()) -> file_meta:path().
build_path(#state{current_path_tokens = CurrentPathTokens, root_file_depth = Depth}) ->
    filename:join(lists:sublist(CurrentPathTokens ++ [<<>>], Depth + 1, length(CurrentPathTokens))).


%% @private
-spec build_continuation_token([result_file_entry()], [file_meta:path()], file_id:file_guid(), boolean()) ->
    token() | undefined.
build_continuation_token(_, _, _, _IsLast = true) -> undefined;
build_continuation_token([], [], _, _) -> undefined;
build_continuation_token(Files, InaccessiblePaths, Guid, _IsLast = false) ->
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
    base64url:encode(json_utils:encode(#{
        <<"guid">> => Guid,
        <<"startAfter">> => StartAfter
    })).


%%%===================================================================
%%% Helper functions operating on #list_result record
%%%===================================================================

%% @private
-spec result_append_inaccessible_path(state(), result()) -> result().
result_append_inaccessible_path(State, Result) ->
    StartAfterPath = State#state.start_after_path,
    case build_path_in_requested_scope(State) of
        out_of_scope -> Result;
        StartAfterPath -> Result;
        Path -> merge_results(Result, #list_result{inaccessible_paths = [Path]})
    end.


%% @private
-spec result_length(result()) -> non_neg_integer().
result_length(#list_result{inaccessible_paths = IP, files = F}) ->
    length(IP) + length(F).


%% @private
-spec merge_results(result(), result()) -> result().
merge_results(#list_result{files = F1, inaccessible_paths = IP1}, #list_result{files = F2, inaccessible_paths = IP2}) ->
    #list_result{
        files = F1 ++ F2,
        inaccessible_paths = IP1 ++ IP2
    }.
