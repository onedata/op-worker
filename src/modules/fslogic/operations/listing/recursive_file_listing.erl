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
    list/3,
    pack_options/3
]).

-type prefix() :: binary().
-type token() :: binary().
-type limit() :: file_meta:list_size().
-type entry() :: {file_meta:path(), lfm_attrs:file_attributes()}.
-type empty_binary() :: binary(). % binary with value exactly <<>>
-type progress_marker() :: more | done.

-record(state, {
    %% values provided in options (unchanged during listing)
    start_after_path :: file_meta:path(),
    prefix = <<>> :: prefix(),
    
    % depth of listing root file relative to space root (unchanged during listing)
    root_file_depth :: non_neg_integer(),
    
    %% values that are modified during listing
    % number of files that are still required to be listed
    limit :: limit(),
    % tokens of start_after_path trimmed to be relative to currently listed file
    start_after_path_tokens :: [file_meta:name()], 
    % name of last listed file, when in subtree pointed by start_after_path
    prev_start_after_token :: undefined | file_meta:name(),
    % absolute path tokens to currently processed directory
    current_path_tokens :: [file_meta:name()],
    % uuid of parent of currently listed file
    parent_uuid :: file_meta:uuid()
}).

-record(list_result, {
    entries = [] :: [entry()],
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

-export_type([prefix/0, token/0, limit/0, entry/0, options/0]).

-define(LIST_RECURSIVE_BATCH_SIZE, 1000).

%%%===================================================================
%%% API
%%%===================================================================

-spec list(user_ctx:ctx(), file_ctx:ctx(), options())-> fslogic_worker:fuse_response().
list(UserCtx, FileCtx0, #options{start_after = StartAfter, limit = Limit, prefix = Prefix, guid = undefined}) ->
    {FilePath, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),
    [_Sep, SpaceId | FilePathTokens] = filename:split(FilePath),
    StartAfterPathTokens = filename:split(StartAfter),
    {ProgressMarker, #list_result{entries = Files, inaccessible_paths = InaccessiblePaths}} = 
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
            entries = Files,
            inaccessible_paths = InaccessiblePaths,
            continuation_token = build_continuation_token(
                Files, InaccessiblePaths, file_ctx:get_logical_guid_const(FileCtx1), ProgressMarker)
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
    #{<<"guid">> := Guid, <<"startAfter">> := StartAfter} = unpack_token(Token), 
    #options{
        start_after = undefined_to_empty_binary(StartAfter),
        limit = Limit,
        prefix = undefined_to_empty_binary(Prefix),
        guid = Guid
    };
pack_options({start_after, StartAfter}, Limit, Prefix) ->
    #options{
        start_after = undefined_to_empty_binary(StartAfter),
        limit = Limit,
        prefix = undefined_to_empty_binary(Prefix)
    }.

%%%===================================================================
%%% Listing internal functions
%%%===================================================================

%% @private
-spec list_next_file(user_ctx:ctx(), file_ctx:ctx(), state()) ->
    {progress_marker(), result()}.
list_next_file(_UserCtx, _FileCtx, #state{limit = Limit}) when Limit =< 0 ->
    {#list_result{}, false};
list_next_file(UserCtx, FileCtx, State) ->
    Path = build_current_rel_path(State),
    case {matches_prefix(Path, State), State#state.prefix > Path} of
        {false, false} ->
            {done, #list_result{}};
        _ ->
            {ListOpts, UpdatedState} = build_initial_listing_opts(State),
            list_children_next_batch(UserCtx, FileCtx, ListOpts, UpdatedState, #list_result{})
    end.


%% @private
-spec list_children_next_batch(user_ctx:ctx(), file_ctx:ctx(), file_meta:list_opts(), state(), result()) -> 
    {progress_marker(), result()}.
list_children_next_batch(UserCtx, FileCtx, ListOpts, State, AccListResult) ->
    #state{limit = Limit} = State,
    {Children, UpdatedAccListResult, #{is_last := IsLast} = ExtendedInfo, FileCtx2} = 
        case list_children_with_access_check(UserCtx, FileCtx, ListOpts) of
            {ok, C, E, F} -> 
                {C, AccListResult, E, F};
            {error, ?EACCES} ->
                {[], result_append_inaccessible_path(State, AccListResult), #{is_last => true}, FileCtx}
        end,
    {Res, FinalProcessedFileCount} = lists_utils:foldl_while(fun(ChildCtx, {TmpResult, ProcessedFileCount}) ->
        {Marker, ChildBatch} = map_listed_child(
            UserCtx, ChildCtx, file_ctx:get_logical_guid_const(FileCtx2), State#state{
                limit = Limit - result_length(TmpResult)
            }),
        ResToReturn = merge_results(TmpResult, ChildBatch),
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
    case {result_length(Res) >= Limit, IsLast} of
        {true, _} ->
            ProgressMarker = case IsLast and (FinalProcessedFileCount == length(Children)) of
                true -> done;
                false -> more
            end,
            {Res, ProgressMarker};
        {false, true} ->
            {done, Res};
        {false, false} ->
            NextListOpts = maps:with([last_tree, last_name], ExtendedInfo),
            list_children_next_batch(
                UserCtx, FileCtx2, NextListOpts, State#state{limit = Limit - result_length(Res)}, Res)
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
        {Children, ExtendedInfo, FileCtx4} = file_listing:list_children(UserCtx, FileCtx3, ListOpts, CanonicalChildrenWhiteList),
        {ok, Children, ExtendedInfo, FileCtx4}
    catch throw:?EACCES ->
        {error, ?EACCES}
    end.


%% @private
-spec map_listed_child(user_ctx:ctx(), file_ctx:ctx(), file_id:file_guid(), state()) ->
    {progress_marker(), [{file_meta:path(), lfm_attrs:file_attributes()}]}.
map_listed_child(UserCtx, ChildCtx, ListedFileGuid, State) ->
    #state{
        current_path_tokens = CurrentPathTokens,
        prev_start_after_token = PrevStartAfterToken
    } = State,
    case get_file_attrs(UserCtx, ChildCtx) of
        #file_attr{type = ?DIRECTORY_TYPE, name = Name, guid = G} ->
            {ProgressMarker, NextChildrenRes} = list_next_file(UserCtx, ChildCtx,
                State#state{
                    current_path_tokens = CurrentPathTokens ++ [Name],
                    parent_uuid = file_id:guid_to_uuid(G)
                }
            ),
            {ProgressMarker, NextChildrenRes};
        #file_attr{type = _, name = PrevStartAfterToken} ->
            % file pointed by start_after should be ignored in result
            {done, #list_result{}};
        #file_attr{guid = G} = FileAttrs when G == ListedFileGuid ->
            % listing regular file should return this file
            {done, #list_result{entries = build_result_file_entry_list(State, FileAttrs)}};
        #file_attr{type = _, name = Name} = FileAttrs ->
            UpdatedState = State#state{current_path_tokens = CurrentPathTokens ++ [Name]},
            {done, #list_result{entries = build_result_file_entry_list(UpdatedState, FileAttrs)}}
    end.


%%%===================================================================
%%% Helper functions 
%%%===================================================================

%% @private
-spec undefined_to_empty_binary(undefined | T) -> empty_binary() | T.
undefined_to_empty_binary(undefined) -> <<>>;
undefined_to_empty_binary(Other) -> Other.


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
-spec build_result_file_entry_list(state(), lfm_attrs:file_attributes()) -> [entry()].
build_result_file_entry_list(State, FileAttrs) ->
    case build_path_in_requested_scope(State) of
        out_of_scope -> [];
        Path -> [{Path, FileAttrs}]
    end.


%% @private
-spec build_path_in_requested_scope(state()) -> file_meta:path() | out_of_scope.
build_path_in_requested_scope(State) ->
    Path = build_current_rel_path(State),
    case matches_prefix(Path, State) of
        true ->
            case Path of
                <<>> -> <<".">>;
                Path -> Path
            end;
        false ->
            out_of_scope
    end.


%% @private
-spec matches_prefix(file_meta:path(), state()) -> boolean().
matches_prefix(Path, #state{prefix = Prefix}) ->
    str_utils:binary_starts_with(Path, Prefix).


%% @private
-spec build_current_rel_path(state()) -> file_meta:path().
build_current_rel_path(#state{current_path_tokens = CurrentPathTokens, root_file_depth = Depth}) ->
    filename:join(lists:sublist(CurrentPathTokens ++ [<<>>], Depth + 1, length(CurrentPathTokens))).


%% @private
-spec build_continuation_token([entry()], [file_meta:path()], file_id:file_guid(), boolean()) ->
    token() | undefined.
build_continuation_token(_, _, _, _ProgressMarker = done) -> undefined;
build_continuation_token([], [], _, _) -> undefined;
build_continuation_token(Files, InaccessiblePaths, Guid, _ProgressMarker = more) ->
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
    pack_token(#{
        <<"guid">> => Guid,
        <<"startAfter">> => StartAfter
    }).


-spec pack_token(map()) -> token().
pack_token(TokenDetails) ->
    base64url:encode(json_utils:encode(TokenDetails)).


-spec unpack_token(token()) -> map() | no_return().
unpack_token(Token) ->
    try json_utils:decode(base64url:decode(Token)) of
        #{<<"guid">> := _Guid, <<"startAfter">> := _StartAfter} = TokenDetails ->
            TokenDetails;
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
    case build_path_in_requested_scope(State) of
        out_of_scope -> Result;
        StartAfterPath -> Result;
        Path -> merge_results(Result, #list_result{inaccessible_paths = [Path]})
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
