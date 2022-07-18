%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for listing recursively objects in subtree of given root object. 
%%% Objects are listed lexicographically ordered by path. Listed path is relative to given root object.
%%% This module operates on concept of traversable and non-traversable objects:
%%%  - traversable object is an object that by itself is a root to a non-trivial tree 
%%%    (e.g. directory, dataset);
%%%  - non-traversable object is any other object, i.e object that do not contain any subtree 
%%%    that can be listed (e.g regular files).
%%%
%%% All paths user does not have access to are returned under `inaccessible_paths` key.
%%% Available options: 
%%%     * limit - maximum number of entries that will be returned in a single request. 
%%%               For this value both `entries` and `inaccessible_paths` are calculated;
%%%     * start_after - determines starting point of listing i.e. listing will start from 
%%%                     the first object path lexicographically larger and listing will continue 
%%%                     until all subtree is listed/limit is reached;
%%%     * prefix - only objects with paths that begin with this value will be returned;
%%%     * include_traversable - when set to true traversable objects entries will be included 
%%%                             in result, by default only non-traversable objects are listed;
%%% @end
%%%--------------------------------------------------------------------
-module(recursive_listing).
-author("Michal Stanisz").

-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([list/4]).

-type prefix() :: binary().
-type pagination_token() :: binary().
-type limit() :: non_neg_integer().
-type result_entry() :: {path(), object()}.
-type progress_marker() :: more | done.

-record(state, {
    %% values provided in options (unchanged during listing)
    
    % callback module that implements `recursive_listing_behaviour`
    module :: module(),
    % required for checking whether object, that is to be appended to result, is not pointed by 
    % a start_after_path (listing should start AFTER object with this path).
    start_after_path :: path(),   
    prefix = <<>> :: prefix(),
    
    % depth of listing root object relative to space root
    root_object_depth :: non_neg_integer(),
    include_traversable :: boolean(),
    
    %% values that are modified during listing
    
    % number of objects that are still required to be listed
    limit :: limit(),
    % tokens of start_after_path trimmed to be relative to currently listed object
    relative_start_after_path_tokens :: [name()],
    % name of last listed objecy, when in subtree pointed by start_after_path
    last_start_after_token :: name(),
    % absolute path tokens to currently processed traversable object
    current_object_path_tokens :: [name()],
    % id of parent of currently listed object
    parent_id :: object_id(),
    % false if any children batch of current object was listed
    is_first_batch = true :: boolean()
}).

-record(list_result, {
    entries = [] :: [result_entry()],
    inaccessible_paths = [] :: [path()]
}).

% For detailed options description see module doc.
-type options() :: #{
    % NOTE: pagination_token and start_after_path are mutually exclusive
    pagination_token => pagination_token(),
    start_after_path => path(),
    prefix => prefix(),
    limit => limit(),
    include_traversable => boolean()
}.

-type state() :: #state{}.
-type result() :: #list_result{}.
-type record() :: #recursive_listing_result{}.

-export_type([prefix/0, pagination_token/0, limit/0, options/0, result_entry/0, record/0]).

% behaviour types
-type object() :: any().
-type object_id() :: binary().
-type name() :: binary().
-type path() :: binary().
-type object_listing_opts() :: any().

-export_type([object/0, object_id/0, name/0, path/0, object_listing_opts/0]).

-define(LIST_RECURSIVE_BATCH_SIZE, 1000).
-define(DEFAULT_PREFIX, <<>>).
-define(DEFAULT_START_AFTER_PATH, <<>>).
-define(DEFAULT_INCLUDE_TRAVERSABLE, false).

%%%===================================================================
%%% API
%%%===================================================================

-spec list(module(), user_ctx:ctx(), object(), options()) -> record() | no_return().
list(_Module, _UserCtx, _Object, #{pagination_token := _, start_after := _}) ->
    %% TODO VFS-7208 introduce conflicting options error after introducing API errors to fslogic
    throw(?EINVAL);
list(Module, UserCtx, Object, #{pagination_token := PaginationToken} = Options) ->
    {TokenRootId, StartAfter} = unpack_pagination_token(PaginationToken), 
    case Module:get_object_id(Object) of
        TokenRootId -> 
            Options2 = maps:remove(pagination_token, Options#{start_after_path => StartAfter}),
            list(Module, UserCtx, Object, Options2);
        _ -> 
            throw(?EINVAL) % requested listing of other object than token's origin
    end;
list(Module, UserCtx, RootObject, Options) ->
    case prepare_initial_listing_state(Module, UserCtx, RootObject, Options) of
        {ok, ObjectToList, InitialState} ->
            {Entries, InaccessiblePaths, PaginationToken} = case Module:is_traversable_object(ObjectToList) of
                {true, ObjectToList2} ->
                    list_traversable_object(UserCtx, ObjectToList2, InitialState, Module:get_object_id(RootObject));
                {false, ObjectToList2} ->
                    list_non_traversable_object(UserCtx, ObjectToList2, InitialState)
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
        {error, ?EACCES, Path} ->
            #recursive_listing_result{
                inaccessible_paths = [Path],
                entries = []
            }
    end.


%%%===================================================================
%%% Initialization functions
%%%===================================================================

%% @private
-spec prepare_initial_listing_state(module(), user_ctx:ctx(), object(), options()) -> 
    {ok, object(), state()} | nothing_to_list | {error, ?EACCES, path()}.
prepare_initial_listing_state(Module, UserCtx, RootObject, Options) ->
    #{
        start_after_path := GivenStartAfter, limit := Limit, prefix := Prefix, include_traversable := IncludeTraversable
    } = put_defaults(Options),
    PrefixTokens = filename:split(Prefix),
    ParentId = Module:get_parent_id(RootObject, UserCtx),
    case infer_starting_object(Module, PrefixTokens, RootObject, ParentId, UserCtx, []) of
        {ok, ObjectToList, LastPrefixToken} ->
            case infer_start_after(GivenStartAfter, Prefix, LastPrefixToken) of
                {ok, FinalStartAfter} ->
                    {ObjectPath, ObjectToList1} = Module:get_object_path(ObjectToList),
                    [_, SpaceId | ObjectPathTokens] = filename:split(ObjectPath),
                    {RootObjectPath, _RootObject2} = Module:get_object_path(RootObject),
                    [_, SpaceId | RootObjectPathTokens] = filename:split(RootObjectPath),
                    {ok, ObjectToList1, #state{
                        module = Module,
                        % use original start after, so when prefix points to existing object it is not ignored
                        start_after_path = GivenStartAfter, 
                        relative_start_after_path_tokens = filename:split(FinalStartAfter),
                        limit = Limit,
                        include_traversable = IncludeTraversable,
                        current_object_path_tokens = [SpaceId | ObjectPathTokens],
                        last_start_after_token = filename:basename(ObjectPath),
                        parent_id = Module:get_object_id(ObjectToList1),
                        prefix = Prefix,
                        root_object_depth = length(RootObjectPathTokens) + 1,
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
        include_traversable => maps:get(include_traversable, Options, ?DEFAULT_INCLUDE_TRAVERSABLE)
    }.
    

%% @private
-spec infer_starting_object(module(), [name()], object(), binary(), user_ctx:ctx(), [name()]) -> 
    {ok, object(), name()} | nothing_to_list | {error, ?EACCES, path()}.
infer_starting_object(_Module, [], Object, _ParentId, _UserCtx, _RevRelPathTokens) ->
    {ok, Object, <<>>};
infer_starting_object(_Module, [PrefixToken], Object, _ParentId, _UserCtx, _RevRelPathTokens) ->
    % no need to check object access, as it will be checked during actual listing
    {ok, Object, PrefixToken};
infer_starting_object(Module, [PrefixToken | Tail], Object, ParentId, UserCtx, RevRelPathTokens) ->
    Opts = Module:build_listing_opts(PrefixToken, 1, false, ParentId),
    case Module:list_children_with_access_check(Object, Opts, UserCtx) of
        {ok, [NextObject], _E, _Object2} ->
            case Module:get_object_name(NextObject, UserCtx) of
                {PrefixToken, NextObject2} -> 
                    ObjectId = Module:get_object_id(NextObject2),
                    infer_starting_object(
                        Module, Tail, NextObject2, ObjectId, UserCtx, [PrefixToken | RevRelPathTokens]);
                _ -> 
                    nothing_to_list
            end;
        {ok, [], _E, _Object2} ->
            nothing_to_list;
        {error, ?EACCES} ->
            {error, ?EACCES, build_path(lists:reverse(RevRelPathTokens))}
    end.


%% @private
-spec infer_start_after(path(), prefix(), name()) -> {ok, path()} | nothing_to_list.
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
                        false -> nothing_to_list % start after is larger than any object path that could possibly match prefix
                    end;
                false ->
                    nothing_to_list
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list_traversable_object(user_ctx:ctx(), object(), state(), object_id()) ->
    {[result_entry()], [path()], pagination_token()}.
list_traversable_object(UserCtx, Object, InitialState, RootObjectId) ->
    {ProgressMarker, #list_result{entries = Entries, inaccessible_paths = InaccessiblePaths}} =
        process_current_traversable_object(UserCtx, Object, InitialState),
    % use root object id to build token, as this is the actual object, that was requested
    PaginationToken = build_pagination_token(Entries, InaccessiblePaths, RootObjectId, ProgressMarker),
    {Entries, InaccessiblePaths, PaginationToken}.


%% @private
-spec list_non_traversable_object(user_ctx:ctx(), object(), state()) ->
    {[result_entry()], [path()], undefined}.
list_non_traversable_object(UserCtx, Object, #state{module = Module} = State) ->
    {EntryList, InaccessiblePaths} = case Module:check_access(Object, UserCtx) of
        ok ->
            {build_result_object_entry_list(State, Object, <<>>), []};
        {error, ?EACCES} ->
            {[], [<<".">>]}
    end,
    {EntryList, InaccessiblePaths, undefined}.


%% @private
-spec process_current_traversable_object(user_ctx:ctx(), object(), state()) ->
    {progress_marker(), result()}.
process_current_traversable_object(_UserCtx, _Object, #state{limit = Limit}) when Limit =< 0 ->
    {more, #list_result{}};
process_current_traversable_object(UserCtx, Object, State) ->
    Path = build_current_traversable_object_rel_path(State),
    % Traversable objects with path that not match given prefix, but is a prefix of this given prefix must be listed, 
    % as they are ancestor objects to objects matching this given prefix. Needs to be checked as listing 
    % starting object never matches prefix, as it is a parent object to prefix last path token).
    IsAncestorObject = case Path of
        <<".">> -> true; % listing root object
        _ -> str_utils:binary_starts_with(State#state.prefix, Path)
    end,
    case matches_prefix(Path, State) orelse IsAncestorObject of
        true ->
            {ListOpts, UpdatedState} = init_current_traversable_object_processing(State),
            process_current_traversable_object_in_batches(UserCtx, Object, ListOpts, UpdatedState, #list_result{});
        false ->
            {done, #list_result{}}
    end.


%% @private
-spec process_current_traversable_object_in_batches(user_ctx:ctx(), object(), object_listing_opts(), state(), result()) -> 
    {progress_marker(), result()}.
process_current_traversable_object_in_batches(UserCtx, Object, ListOpts, #state{module = Module} = State, AccListResult) ->
    #state{limit = Limit} = State,
    {Children, UpdatedAccListResult, NextListOpts, IsInaccessible, Object2} = 
        case Module:list_children_with_access_check(Object, ListOpts, UserCtx) of
            {ok, C, LO, O} -> 
                {C, append_traversable_object(State, O, AccListResult), LO, false, O};
            {error, ?EACCES} ->
                {[], result_append_inaccessible_path(State, AccListResult), undefined, true, Object}
        end,
    
    {Res, FinalProcessedObjectCount} = lists_utils:foldl_while(fun(ChildObject, {TmpResult, ProcessedObjectCount}) ->
        {Marker, SubtreeResult} = process_subtree(UserCtx, ChildObject, State#state{
            limit = Limit - result_length(TmpResult)
        }),
        ResToReturn = merge_results(TmpResult, SubtreeResult),
        ObjectProcessedIncrement = case Marker of
            done -> 1;
            more -> 0
        end,
        ToReturn = {ResToReturn, ProcessedObjectCount + ObjectProcessedIncrement},
        case result_length(ResToReturn) >= Limit of
            true -> {halt, ToReturn};
            false -> {cont, ToReturn}
        end
    end, {UpdatedAccListResult, 0}, Children),
    
    ResultLength = result_length(Res),
    case ResultLength > Limit of
        true -> 
            ?critical(
                "Listed more entries than requested in recursive listing of object: ~p~n"
                "state: ~p~noptions: ~p~nlimit: ~p, listed: ~p", 
                [Module:get_object_id(Object), State, ListOpts, Limit, ResultLength]),
            throw(?ERROR_INTERNAL_SERVER_ERROR);
        _ -> 
            ok
    end, 
    
    case {ResultLength, IsInaccessible orelse Module:is_listing_finished(NextListOpts)} of
        {Limit, IsFinished} ->
            ProgressMarker = case IsFinished and (FinalProcessedObjectCount == length(Children)) of
                true -> done;
                false -> more
            end,
            {ProgressMarker, Res};
        {_, true} ->
            {done, Res};
        {_, false} ->
            UpdatedState = State#state{
                limit = Limit - result_length(Res), 
                is_first_batch = false
            },
            process_current_traversable_object_in_batches(UserCtx, Object2, NextListOpts, UpdatedState, Res)
    end.
    

%% @private
-spec process_subtree(user_ctx:ctx(), object(), state()) ->
    {progress_marker(), result()}.
process_subtree(_UserCtx, _Object, #state{limit = 0}) ->
    {more, #list_result{}};
process_subtree(UserCtx, Object, #state{current_object_path_tokens = CurrentPathTokens, module = Module} = State) ->
    {Name, Object2} = Module:get_object_name(Object, UserCtx),
    case Module:is_traversable_object(Object2) of
        {true, Object3} ->
            {ProgressMarker, NextChildrenRes} = process_current_traversable_object(UserCtx, Object,
                State#state{
                    current_object_path_tokens = CurrentPathTokens ++ [Name],
                    parent_id = Module:get_object_id(Object3)
                }
            ),
            {ProgressMarker, NextChildrenRes};
        {false, Object3} ->
            UpdatedState = State#state{current_object_path_tokens = CurrentPathTokens},
            {done, #list_result{entries = build_result_object_entry_list(UpdatedState, Object3, Name)}}
    end.


%%%===================================================================
%%% Helper functions 
%%%===================================================================
    
%% @private
-spec init_current_traversable_object_processing(state()) -> {object_listing_opts(), state()}.
init_current_traversable_object_processing(#state{relative_start_after_path_tokens = []} = State) ->
    {#{tune_for_large_continuous_listing => true}, State};
init_current_traversable_object_processing(#state{
    module = Module,
    relative_start_after_path_tokens = [CurrentStartAfterToken | NextStartAfterTokens],
    last_start_after_token = LastStartAfterToken,
    current_object_path_tokens = CurrentPathTokens,
    parent_id = ParentId
} = State) ->
    InitialOpts = Module:build_listing_opts(undefined, ?LIST_RECURSIVE_BATCH_SIZE, true, ParentId),
    %% As long as the currently processed path is within the start_after_path, we can start
    %% listing from the specific object name (CurrentStartAfterToken), as all names lexicographically
    %% smaller should not be included in the results. Otherwise, the whole traversable object should 
    %% be listed and processed.
    case lists:last(CurrentPathTokens) == LastStartAfterToken of
        true ->
            ListingOpts = Module:build_listing_opts(CurrentStartAfterToken, ?LIST_RECURSIVE_BATCH_SIZE, true, ParentId),
            UpdatedState = State#state{
                relative_start_after_path_tokens = NextStartAfterTokens, 
                last_start_after_token = CurrentStartAfterToken
            },
            {ListingOpts, UpdatedState};
        _ ->
            {InitialOpts, State#state{relative_start_after_path_tokens = []}}
    end.


%% @private
-spec build_result_object_entry_list(state(), object(), name()) -> 
    [result_entry()].
build_result_object_entry_list(#state{start_after_path = StartAfterPath} = State, Object, Name) ->
    case build_rel_path_in_current_object_with_prefix_check(State, Name) of
        false -> [];
        {true, StartAfterPath} -> [];
        {true, Path} -> [{Path, Object}]
    end.


%% @private
-spec build_current_traversable_object_rel_path_with_prefix_check(state()) -> {true, path()} | false.
build_current_traversable_object_rel_path_with_prefix_check(State) ->
    build_rel_path_in_current_object_with_prefix_check(State, <<>>).


%% @private
-spec build_rel_path_in_current_object_with_prefix_check(state(), name()) -> 
    {true, path()} | false.
build_rel_path_in_current_object_with_prefix_check(State, Name) ->
    Path = build_rel_path_in_current_object(State, Name),
    case matches_prefix(Path, State) of
        true -> {true, Path};
        false -> false
    end.


%% @private
-spec matches_prefix(path(), state()) -> boolean().
matches_prefix(Path, #state{prefix = Prefix}) ->
    str_utils:binary_starts_with(Path, Prefix).


%% @private
-spec build_current_traversable_object_rel_path(state()) -> path().
build_current_traversable_object_rel_path(State) ->
    build_rel_path_in_current_object(State, <<>>).


%% @private
-spec build_rel_path_in_current_object(state(), name()) -> path().
build_rel_path_in_current_object(#state{current_object_path_tokens = CurrentPathTokens, root_object_depth = Depth}, Name) ->
    build_path(lists:sublist(CurrentPathTokens ++ [Name], Depth + 1, length(CurrentPathTokens))).


%% @private
-spec build_path([name()]) -> path().
build_path([]) ->
    <<".">>;
build_path([<<>>]) ->
    <<".">>;
build_path(Tokens) ->
    filename:join(Tokens).


%% @private
-spec build_pagination_token([result_entry()], [path()], object_id(), progress_marker()) ->
    pagination_token() | undefined.
build_pagination_token(_, _, _, _ProgressMarker = done) -> undefined;
build_pagination_token([], [], _, _) -> undefined;
build_pagination_token(Entries, InaccessiblePaths, RootObjectId, _ProgressMarker = more) ->
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
    pack_pagination_token(RootObjectId, StartAfter).


%% @private
-spec pack_pagination_token(object_id(), path()) -> pagination_token().
pack_pagination_token(ObjectId, StartAfter) ->
    mochiweb_base64url:encode(json_utils:encode(#{
        <<"objectId">> => ObjectId, 
        <<"startAfter">> => StartAfter
    })).


%% @private
-spec unpack_pagination_token(pagination_token()) -> {object_id(), path()} | no_return().
unpack_pagination_token(Token) ->
    try json_utils:decode(mochiweb_base64url:decode(Token)) of
        #{<<"objectId">> := ObjectId, <<"startAfter">> := StartAfter} ->
            {ObjectId, StartAfter};
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
    case build_current_traversable_object_rel_path_with_prefix_check(State) of
        false -> Result;
        {true, StartAfterPath} -> Result;
        {true, Path} -> merge_results(Result, #list_result{inaccessible_paths = [Path]})
    end.


%% @private
-spec result_length(result()) -> non_neg_integer().
result_length(#list_result{inaccessible_paths = IP, entries = E}) ->
    length(IP) + length(E).


%% @private
-spec merge_results(result(), result()) -> result().
merge_results(
    #list_result{entries = E1, inaccessible_paths = IP1}, 
    #list_result{entries = E2, inaccessible_paths = IP2}
) ->
    #list_result{
        entries = E1 ++ E2,
        inaccessible_paths = IP1 ++ IP2
    }.


%% @private
-spec append_traversable_object(state(), object(), result()) -> result().
append_traversable_object(
    #state{include_traversable = true, is_first_batch = true} = State, 
    Object, 
    ListResult
) ->
    #list_result{entries = Entries} = ListResult,
    case {build_current_traversable_object_rel_path_with_prefix_check(State), State#state.start_after_path} of
        {{true, <<".">> = Path}, <<>>} -> 
            ListResult#list_result{entries = Entries ++ [{Path, Object}]};
        {{true, <<".">>}, _StartAfter} -> 
            ListResult;
        {{true, Path}, StartAfter} ->
            case Path =< StartAfter of
                true -> ListResult;
                false -> ListResult#list_result{entries = Entries ++ [{Path, Object}]}
            end;
        {false, _} ->
            ListResult
    end;
append_traversable_object(_State, _Object, ListResult) ->
    ListResult.
