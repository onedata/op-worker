%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for listing recursively nodes in subtree of given root node. 
%%% Nodes are listed lexicographically ordered by path. Listed path is relative to given root node.
%%% This module operates on concept of branching and non-branching nodes:
%%%  - branching node is a node that by itself is a root to a non-trivial tree 
%%%    (e.g. directory, dataset);
%%%  - non-branching node is any other node, i.e node that do not contain any subtree 
%%%    that can be listed (e.g regular files).
%%%
%%% It is assumed that there is access to root node. All paths user does not have access to are 
%%% returned under `inaccessible_paths` key.
%%% Available options: 
%%%     * pagination_token
%%%         determines the starting point for listing. The listing will start from the next page 
%%%         (batch) of entries which follows the page previously obtained along with the 
%%%         corresponding pagination_token. Cannot be provided alongside the start_after parameter;
%%%     * start_after
%%%         determines starting point of listing i.e. listing will start from the first node path 
%%%         lexicographically larger and listing will continue until all subtree is listed/limit 
%%%         is reached. Cannot be provided alongside the pagination_token parameter.
%%%     * prefix
%%%         only nodes with paths that begin with this value will be returned;
%%%     * limit
%%%         maximum number of entries that will be returned in a single request. 
%%%         For this value both `entries` and `inaccessible_paths` are calculated;
%%%     * include_branching_nodes
%%%         when set to true branching nodes entries will be included in result; by default only 
%%%         non-branching nodes are listed;
%%% @end
%%%--------------------------------------------------------------------
-module(recursive_listing).
-author("Michal Stanisz").

-include("modules/fslogic/recursive_listing.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([list/4]).

-type prefix() :: binary().
-type pagination_token() :: binary().
-type progress_marker() :: more | done.

-record(state, {
    %% values provided in options (unchanged during listing)
    
    % module that implements behaviour callbacks
    module :: module(),
    % required for checking whether node, that is to be appended to result, is not pointed by 
    % a start_after_path (listing should start AFTER node with this path).
    start_after_path :: node_path(),   
    prefix = <<>> :: prefix(),
    
    % depth of listing root node relative to space root
    root_node_depth :: non_neg_integer(),
    include_branching_nodes :: boolean(),
    
    %% values that are modified during listing
    
    % number of nodes that are still required to be listed
    limit :: limit(),
    % tokens of start_after_path trimmed to be relative to currently listed node
    relative_start_after_path_tokens :: [node_name()],
    % name of last listed node, when in subtree pointed by start_after_path
    last_start_after_token :: node_name(),
    % currently processed tree node
    current_node :: tree_node(),
    % absolute path tokens to currently processed branching node
    current_node_path_tokens :: [node_name()],
    % false if any children batch of current node was listed
    is_first_batch = true :: boolean()
}).

-record(result_accumulator, {
    entries = [] :: [result_entry()],
    inaccessible_paths = [] :: [node_path()]
}).

% For detailed options description see module doc.
-type options() :: #{
    % NOTE: pagination_token and start_after_path are mutually exclusive
    pagination_token => pagination_token(),
    start_after_path => node_path(),
    prefix => prefix(),
    limit => limit(),
    include_branching_nodes => boolean()
}.

-type state() :: #state{}.
-type accumulator() :: #result_accumulator{}.
-type result_entry(NodePath, TreeNode) :: {NodePath, TreeNode}.
-type result_entry() :: result_entry(node_path(), tree_node()).
-type result(NodePath, Entry) :: #recursive_listing_result{
    entries :: [Entry], 
    inaccessible_paths :: [NodePath]
}.
-type result() :: result(node_path(), result_entry()).

-export_type([prefix/0, pagination_token/0, options/0, result_entry/0, result_entry/2, result/2]).

% behaviour types
-type tree_node() :: any().
-type node_id() :: binary(). % value used for checking correctness of pagination_token
-type node_name() :: binary(). % value from which result path is built
-type node_path() :: binary().
-type node_iterator() :: any().
-type limit() :: non_neg_integer().

-export_type([tree_node/0, node_id/0, node_name/0, node_path/0, node_iterator/0, limit/0]).

-define(LIST_RECURSIVE_BATCH_SIZE, 1000).
-define(DEFAULT_PREFIX, <<>>).
-define(DEFAULT_START_AFTER_PATH, <<>>).
-define(DEFAULT_INCLUDE_BRANCHING_NODES, false).


%%%===================================================================
%%% API
%%%===================================================================

-spec list(module(), user_ctx:ctx(), tree_node(), options()) -> result() | no_return().
list(_Module, _UserCtx, _Node, #{pagination_token := _, start_after := _}) ->
    %% TODO VFS-7208 introduce conflicting options error after introducing API errors to fslogic
    throw(?EINVAL);
list(Module, UserCtx, Node, #{pagination_token := PaginationToken} = Options) ->
    {TokenRootId, StartAfter} = unpack_pagination_token(PaginationToken), 
    case Module:get_node_id(Node) of
        {TokenRootId, Node2} -> 
            Options2 = maps:remove(pagination_token, Options#{start_after_path => StartAfter}),
            list(Module, UserCtx, Node2, Options2);
        _ -> 
            throw(?EINVAL) % requested listing of other node than token's origin
    end;
list(Module, UserCtx, RootNode, Options) ->
    case prepare_initial_listing_state(Module, UserCtx, RootNode, Options) of
        {ok, #state{current_node = NodeToList} = InitialState} ->
            {Entries, InaccessiblePaths, PaginationToken} = case Module:is_branching_node(NodeToList) of
                {true, NodeToList2} ->
                    {ProgressMarker, E, IP} =
                        list_branching_node(UserCtx, InitialState#state{current_node = NodeToList2}),
                    % use root node id to build token, as this is the actual node, that was requested
                    {RootNodeId, _} = Module:get_node_id(RootNode),
                    {E, IP, build_pagination_token(E, IP, RootNodeId, ProgressMarker)};
                {false, NodeToList2} ->
                    {build_result_node_entry_list(
                        InitialState#state{current_node = NodeToList2}, <<>>), [], undefined};
                not_found ->
                    #recursive_listing_result{
                        inaccessible_paths = [],
                        entries = []
                    }
            end,
            #recursive_listing_result{
                entries = Entries,
                inaccessible_paths = InaccessiblePaths,
                pagination_token = PaginationToken
            };
        nothing_to_list ->
            #recursive_listing_result{
                inaccessible_paths = [],
                entries = []
            };
        {no_access, Path} ->
            #recursive_listing_result{
                inaccessible_paths = [Path],
                entries = []
            }
    end.


%%%===================================================================
%%% Initialization functions
%%%===================================================================

%% @private
-spec prepare_initial_listing_state(module(), user_ctx:ctx(), tree_node(), options()) -> 
    {ok, state()} | nothing_to_list | {no_access, node_path()}.
prepare_initial_listing_state(Module, UserCtx, RootNode, Options) ->
    #{
        start_after_path := GivenStartAfter, limit := Limit, 
        prefix := Prefix, include_branching_nodes := IncludeBranching
    } = put_defaults(Options),
    PrefixTokens = filename:split(Prefix),
    case infer_starting_node(Module, PrefixTokens, RootNode, UserCtx, []) of
        {ok, NodeToList, LastPrefixToken} ->
            case infer_start_after(GivenStartAfter, Prefix, LastPrefixToken) of
                {ok, FinalStartAfter} ->
                    % all docs on node path must be synced as infer_starting_node returned ok
                    % therefore no need to check if not_found
                    {NodePathTokens, NodeToList2} = Module:get_node_path_tokens(NodeToList),
                    {RootNodePathTokens, _RootNode2} = Module:get_node_path_tokens(RootNode),
                    {ok, #state{
                        module = Module,
                        % use original start after, so when prefix points to existing node it is not ignored
                        start_after_path = GivenStartAfter, 
                        relative_start_after_path_tokens = filename:split(FinalStartAfter),
                        limit = Limit,
                        include_branching_nodes = IncludeBranching,
                        current_node = NodeToList2,
                        current_node_path_tokens = NodePathTokens,
                        last_start_after_token = lists:last(NodePathTokens),
                        prefix = Prefix,
                        root_node_depth = length(RootNodePathTokens),
                        is_first_batch = true
                    }};
                nothing_to_list ->
                    nothing_to_list
            end;
        Other ->
            Other
    end.


%% @private
-spec put_defaults(options()) -> options().
put_defaults(Options) ->
    #{
        start_after_path => maps:get(start_after_path, Options, ?DEFAULT_START_AFTER_PATH),
        limit => maps:get(limit, Options, ?LIST_RECURSIVE_BATCH_SIZE),
        prefix => maps:get(prefix, Options, ?DEFAULT_PREFIX),
        include_branching_nodes => maps:get(include_branching_nodes, Options, ?DEFAULT_INCLUDE_BRANCHING_NODES)
    }.
    

%% @private
-spec infer_starting_node(module(), [node_name()], tree_node(), user_ctx:ctx(), [node_name()]) -> 
    {ok, tree_node(), node_name()} | nothing_to_list | {no_access, node_path()}.
infer_starting_node(_Module, [], Node, _UserCtx, _RevRelPathTokens) ->
    {ok, Node, <<>>};
infer_starting_node(_Module, [PrefixToken], Node, _UserCtx, _RevRelPathTokens) ->
    % no need to check node access, as it will be checked during actual listing
    {ok, Node, PrefixToken};
infer_starting_node(Module, [PrefixToken | Tail], Node, UserCtx, RevRelPathTokens) ->
    NodeIterator = Module:init_node_iterator(Node, PrefixToken, 1),
    case Module:get_next_batch(NodeIterator, UserCtx) of
        {_, [NextNode], _Iterator} ->
            case Module:get_node_name(NextNode, UserCtx) of
                {PrefixToken, NextNode2} ->
                    infer_starting_node(
                        Module, Tail, NextNode2, UserCtx, [PrefixToken | RevRelPathTokens]);
                _ ->
                    % if not_found then ONLY node that could be on prefix path is not yet fully synced
                    nothing_to_list
            end;
        {_, [], _Iterator} ->
            nothing_to_list;
        no_access ->
            {no_access, build_path(lists:reverse(RevRelPathTokens))}
    end.


%% @private
-spec infer_start_after(node_path(), prefix(), node_name()) -> {ok, node_path()} | nothing_to_list.
infer_start_after(GivenStartAfter, Prefix, LastPrefixToken) ->
    PrefixTokens = filename:split(Prefix),
    case {PrefixTokens, GivenStartAfter =< Prefix} of
        {[], _} ->
            {ok, GivenStartAfter};
        {[PrefixToken], _} ->
            {ok, max(GivenStartAfter, PrefixToken)};
        {_, true} ->
            {ok, LastPrefixToken};
        {_, false} ->
            PrefixWithoutLastToken = build_path(lists:sublist(PrefixTokens, length(PrefixTokens) - 1)),
            case filepath_utils:is_descendant(GivenStartAfter, PrefixWithoutLastToken) of
                {true, RelStartAfter} ->
                    case str_utils:binary_starts_with(RelStartAfter, LastPrefixToken) of
                        true -> {ok, RelStartAfter};
                        false -> nothing_to_list % start after is larger than any node path that could possibly match prefix
                    end;
                false ->
                    nothing_to_list
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list_branching_node(user_ctx:ctx(), state()) ->
    {more | done, [result_entry()], [node_path()]}.
list_branching_node(UserCtx, InitialState) ->
    {ProgressMarker, #result_accumulator{entries = Entries, inaccessible_paths = InaccessiblePaths}} =
        process_current_branching_node(UserCtx, InitialState),
    {ProgressMarker, Entries, InaccessiblePaths}.


%% @private
-spec process_current_branching_node(user_ctx:ctx(), state()) ->
    {progress_marker(), accumulator()}.
process_current_branching_node(_UserCtx, #state{limit = Limit}) when Limit =< 0 ->
    {more, #result_accumulator{}};
process_current_branching_node(UserCtx, State) ->
    Path = build_current_branching_node_rel_path(State),
    % branching nodes with path that not match given prefix, but is a prefix of this given prefix must be listed, 
    % as they are ancestor nodes to nodes matching this given prefix. Needs to be checked as listing 
    % starting node never matches prefix, as it is a parent node to prefix last path token).
    IsAncestorNode = case Path of
        <<".">> -> true; % listing root node
        _ -> str_utils:binary_starts_with(State#state.prefix, Path)
    end,
    case matches_prefix(Path, State) orelse IsAncestorNode of
        true ->
            {NodeIterator, UpdatedState} = init_current_branching_node_processing(State),
            process_current_branching_node_in_batches(UserCtx, NodeIterator, UpdatedState, #result_accumulator{});
        false ->
            {done, #result_accumulator{}}
    end.


%% @private
-spec process_current_branching_node_in_batches(
    user_ctx:ctx(), node_iterator(), state(), accumulator()
) -> 
    {progress_marker(), accumulator()}.
process_current_branching_node_in_batches(UserCtx, NodeIterator, State, ResultAcc) ->
    #state{limit = Limit, module = Module} = State,
    {ListingProgressMarker, ListedChildren, UpdatedResultAcc, NextIterator} = 
        case Module:get_next_batch(NodeIterator, UserCtx) of
            {PM, Batch, Iter} -> 
                {PM, Batch, append_branching_node(State, ResultAcc), Iter};
            no_access ->
                {done, [], result_append_inaccessible_path(State, ResultAcc), undefined}
        end,
    
    {Res, FinalProcessedChildrenCount} = lists_utils:foldl_while(fun(ChildNode, {TmpResultAcc, ProcessedNodeCount}) ->
        {Marker, ChildResultAcc} = process_child(UserCtx, ChildNode, State#state{
            limit = Limit - result_length(TmpResultAcc)
        }),
        ResToReturn = merge_results(TmpResultAcc, ChildResultAcc),
        NodeProcessedIncrement = case Marker of
            done -> 1;
            more -> 0
        end,
        ToReturn = {ResToReturn, ProcessedNodeCount + NodeProcessedIncrement},
        case result_length(ResToReturn) >= Limit of
            true -> {halt, ToReturn};
            false -> {cont, ToReturn}
        end
    end, {UpdatedResultAcc, 0}, ListedChildren),
    
    ResultLength = result_length(Res),
    case ResultLength > Limit of
        true -> 
            ?critical(?autoformat_with_msg("Listed more entries than requested in recursive listing",
                [Limit, ResultLength, NodeIterator, State]
            )),
            throw(?ERROR_INTERNAL_SERVER_ERROR);
        _ -> 
            ok
    end, 
    
    case {ResultLength, ListingProgressMarker} of
        {Limit, _} ->
            ProgressMarker = case 
                (ListingProgressMarker == done) and (FinalProcessedChildrenCount == length(ListedChildren)) 
            of
                true -> done;
                false -> more
            end,
            {ProgressMarker, Res};
        {_, done} ->
            {done, Res};
        {_, more} ->
            UpdatedState = State#state{
                is_first_batch = false
            },
            process_current_branching_node_in_batches(UserCtx, NextIterator, UpdatedState, Res)
    end.
    

%% @private
-spec process_child(user_ctx:ctx(), tree_node(), state()) ->
    {progress_marker(), accumulator()}.
process_child(_UserCtx, _Node, #state{limit = 0}) -> 
    {more, #result_accumulator{}};
process_child(UserCtx, Node, #state{current_node_path_tokens = CurrentPathTokens, module = Module} = State) ->
    case Module:get_node_name(Node, UserCtx) of
        {Name, Node2} ->
            case Module:is_branching_node(Node2) of
                {true, Node3} ->
                    {ProgressMarker, NextChildrenRes} = process_current_branching_node(UserCtx,
                        State#state{
                            current_node = Node3,
                            current_node_path_tokens = CurrentPathTokens ++ [Name]
                        }
                    ),
                    {ProgressMarker, NextChildrenRes};
                {false, Node3} ->
                    UpdatedState = State#state{current_node_path_tokens = CurrentPathTokens, current_node = Node3},
                    {done, #result_accumulator{entries = build_result_node_entry_list(UpdatedState, Name)}};
                not_found ->
                    {done, #result_accumulator{}}
            end;
        not_found ->
            {done, #result_accumulator{}}
    end.


%%%===================================================================
%%% Helper functions 
%%%===================================================================

%% @private
-spec init_current_branching_node_processing(state()) -> {node_iterator(), state()}.
init_current_branching_node_processing(#state{relative_start_after_path_tokens = []} = State) ->
    #state{module = Module, current_node = Node} = State,
    {Module:init_node_iterator(Node, undefined, ?LIST_RECURSIVE_BATCH_SIZE), State};
init_current_branching_node_processing(#state{
    module = Module,
    relative_start_after_path_tokens = [CurrentStartAfterToken | NextStartAfterTokens],
    last_start_after_token = LastStartAfterToken,
    current_node = Node,
    current_node_path_tokens = CurrentPathTokens
} = State) ->
    %% As long as the currently processed path is within the start_after_path, we can start
    %% listing from the specific node name (CurrentStartAfterToken), as all names lexicographically
    %% smaller should not be included in the results. Otherwise, the whole branching node should 
    %% be listed and processed.
    case lists:last(CurrentPathTokens) == LastStartAfterToken of
        true ->
            NodeIterator = Module:init_node_iterator(
                Node, CurrentStartAfterToken, ?LIST_RECURSIVE_BATCH_SIZE),
            UpdatedState = State#state{
                relative_start_after_path_tokens = NextStartAfterTokens, 
                last_start_after_token = CurrentStartAfterToken
            },
            {NodeIterator, UpdatedState};
        _ ->
            NodeIterator = Module:init_node_iterator(
                Node, undefined, ?LIST_RECURSIVE_BATCH_SIZE),
            {NodeIterator, State#state{relative_start_after_path_tokens = []}}
    end.


%% @private
-spec build_result_node_entry_list(state(), node_name()) -> 
    [result_entry()].
build_result_node_entry_list(#state{start_after_path = StartAfterPath, current_node = Node} = State, Name) ->
    case build_rel_path_in_current_node_with_prefix_check(State, Name) of
        false -> [];
        {true, StartAfterPath} -> [];
        {true, Path} -> [{Path, Node}]
    end.


%% @private
-spec build_current_branching_node_rel_path_with_prefix_check(state()) -> {true, node_path()} | false.
build_current_branching_node_rel_path_with_prefix_check(State) ->
    build_rel_path_in_current_node_with_prefix_check(State, <<>>).


%% @private
-spec build_rel_path_in_current_node_with_prefix_check(state(), node_name()) -> 
    {true, node_path()} | false.
build_rel_path_in_current_node_with_prefix_check(State, Name) ->
    Path = build_rel_path_in_current_node(State, Name),
    case matches_prefix(Path, State) of
        true -> {true, Path};
        false -> false
    end.


%% @private
-spec matches_prefix(node_path(), state()) -> boolean().
matches_prefix(Path, #state{prefix = Prefix}) ->
    str_utils:binary_starts_with(Path, Prefix).


%% @private
-spec build_current_branching_node_rel_path(state()) -> node_path().
build_current_branching_node_rel_path(State) ->
    build_rel_path_in_current_node(State, <<>>).


%% @private
-spec build_rel_path_in_current_node(state(), node_name()) -> node_path().
build_rel_path_in_current_node(#state{current_node_path_tokens = CurrentPathTokens, root_node_depth = Depth}, Name) ->
    build_path(lists:sublist(CurrentPathTokens ++ [Name], Depth + 1, length(CurrentPathTokens))).


%% @private
-spec build_path([node_name()]) -> node_path().
build_path([]) ->
    <<".">>;
build_path([<<>>]) ->
    <<".">>;
build_path(Tokens) ->
    filename:join(Tokens).


%% @private
-spec build_pagination_token([result_entry()], [node_path()], node_id(), progress_marker()) ->
    pagination_token() | undefined.
build_pagination_token(_, _, _, _ProgressMarker = done) -> undefined;
build_pagination_token([], [], _, _) -> undefined;
build_pagination_token(Entries, InaccessiblePaths, RootNodeId, _ProgressMarker = more) ->
    StartAfter = case {InaccessiblePaths, Entries} of
        {[], _} ->
            {T, _} = lists:last(Entries),
            T;
        {_, []} ->
            lists:last(InaccessiblePaths);
        {_, _} ->
            {T, _} = lists:last(Entries),
            max(T, lists:last(InaccessiblePaths))
    end,
    pack_pagination_token(RootNodeId, StartAfter).


%% @private
-spec pack_pagination_token(node_id(), node_path()) -> pagination_token().
pack_pagination_token(NodeId, StartAfter) ->
    mochiweb_base64url:encode(json_utils:encode(#{
        <<"nodeId">> => NodeId, 
        <<"startAfter">> => StartAfter
    })).


%% @private
-spec unpack_pagination_token(pagination_token()) -> {node_id(), node_path()} | no_return().
unpack_pagination_token(Token) ->
    try json_utils:decode(mochiweb_base64url:decode(Token)) of
        #{<<"nodeId">> := NodeId, <<"startAfter">> := StartAfter} ->
            {NodeId, StartAfter};
        _ ->
            throw({error, ?EINVAL})
    catch _:_ ->
        throw({error, ?EINVAL})
    end.

%%%===================================================================
%%% Helper functions operating on #result_accumulator record
%%%===================================================================

%% @private
-spec result_append_inaccessible_path(state(), accumulator()) -> accumulator().
result_append_inaccessible_path(State, Result) ->
    StartAfterPath = State#state.start_after_path,
    case build_current_branching_node_rel_path_with_prefix_check(State) of
        false -> Result;
        {true, StartAfterPath} -> Result;
        {true, Path} -> merge_results(Result, #result_accumulator{inaccessible_paths = [Path]})
    end.


%% @private
-spec result_length(accumulator()) -> non_neg_integer().
result_length(#result_accumulator{inaccessible_paths = IP, entries = E}) ->
    length(IP) + length(E).


%% @private
-spec merge_results(accumulator(), accumulator()) -> accumulator().
merge_results(
    #result_accumulator{entries = E1, inaccessible_paths = IP1}, 
    #result_accumulator{entries = E2, inaccessible_paths = IP2}
) ->
    #result_accumulator{
        entries = E1 ++ E2,
        inaccessible_paths = IP1 ++ IP2
    }.


%% @private
-spec append_branching_node(state(), accumulator()) -> accumulator().
append_branching_node(
    #state{include_branching_nodes = true, is_first_batch = true} = State, 
    ResultAcc
) ->
    #state{current_node = Node, start_after_path = StartAfterPath} = State, 
    #result_accumulator{entries = Entries} = ResultAcc,
    case {build_current_branching_node_rel_path_with_prefix_check(State), StartAfterPath} of
        {{true, <<".">> = Path}, <<>>} -> 
            ResultAcc#result_accumulator{entries = Entries ++ [{Path, Node}]};
        {{true, <<".">>}, _StartAfter} ->
            ResultAcc;
        {{true, Path}, StartAfter} ->
            case Path =< StartAfter of
                true -> ResultAcc;
                false -> ResultAcc#result_accumulator{entries = Entries ++ [{Path, Node}]}
            end;
        {false, _} ->
            ResultAcc
    end;
append_branching_node(_State, ListResult) ->
    ListResult.
