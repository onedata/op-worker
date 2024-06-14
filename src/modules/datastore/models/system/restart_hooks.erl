%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Stores hooks to be called on system restart after connecting to zone.
%%% Hooks are stored per space.
%%%
%%% NOTE: hooks should be executed on restart before any request is handled
%%% so there are no other mechanism that prevent add/execute races.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(restart_hooks).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([add_hook/5, delete_hook/1, maybe_execute_hooks/0]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

%% Export for rpc
-export([maybe_execute_hooks_on_node/0]).


-record(hook, {
    module :: module(),
    function :: atom(),
    % list of arbitrary args encoded using term_to_binary/1
    encoded_args :: binary()
}).


-type id() :: binary().
-type hooks() :: #{id() => #hook{}}.
-export_type([hooks/0]).


-define(CTX, #{
    model => ?MODULE
}).

-define(DOC_KEY, <<"restart_hooks">>).
-define(MAX_HOOKS_NUMBER, 1000).
-define(RESTART_HOOKS_STATUS_KEY, restart_hooks_status).


%%%===================================================================
%%% API
%%%===================================================================

-spec add_hook(id(), module(), atom(), [term()], allow_override | forbid_override) ->
    ok | {error, already_exists | too_many_hooks}.
add_hook(Identifier, Module, Function, Args, OverrideOption) ->
    EncodedArgs = term_to_binary(Args),
    Hook = #hook{
        module = Module,
        function = Function,
        encoded_args = EncodedArgs
    },

    UpdateAns = datastore_model:update(?CTX, ?DOC_KEY, fun
        (#restart_hooks{hooks = Hooks} = Record) when map_size(Hooks) < ?MAX_HOOKS_NUMBER ->
            case {maps:get(Identifier, Hooks, undefined), OverrideOption} of
                {undefined, _} -> {ok, Record#restart_hooks{hooks = Hooks#{Identifier => Hook}}};
                {_, allow_override} -> {ok, Record#restart_hooks{hooks = Hooks#{Identifier => Hook}}};
                {_, forbid_override} -> {error, already_exists}
            end;
        (_) ->
            {error, too_many_hooks}
    end, #restart_hooks{hooks = #{Identifier => Hook}}),

    case UpdateAns of
        {ok, _} -> ok;
        {error, already_exists} -> {error, already_exists};
        {error, too_many_hooks} -> {error, too_many_hooks}
    end.


-spec delete_hook(id()) -> ok.
delete_hook(Identifier) ->
    ok = ?extract_ok(?ok_if_not_found(datastore_model:update(?CTX, ?DOC_KEY, fun(#restart_hooks{hooks = Hooks} = Record) ->
        {ok, Record#restart_hooks{hooks = maps:remove(Identifier, Hooks)}}
    end))).


-spec maybe_execute_hooks() -> ok.
maybe_execute_hooks() ->
    Node = consistent_hashing:get_assigned_node(?DOC_KEY),
    ok = erpc:call(Node, ?MODULE, maybe_execute_hooks_on_node, []).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec maybe_execute_hooks_on_node() -> ok.
maybe_execute_hooks_on_node() ->
    critical_section:run(?MODULE, fun() ->
        node_cache:acquire(?RESTART_HOOKS_STATUS_KEY, fun() ->
            execute_hooks(),
            {ok, executed, infinity}
        end)
    end),
    ok.


-spec execute_hooks() -> ok.
execute_hooks() ->
    RestartHooks = case datastore_model:get(?CTX, ?DOC_KEY) of
        {ok, #document{value = #restart_hooks{hooks = Hooks}}} ->
            Hooks;
        {error, not_found} ->
            #{}
    end,

    maps:foreach(fun(Identifier, #hook{module = Module, function = Function, encoded_args = EncodedArgs}) ->
        try
            ok = erlang:apply(Module, Function, binary_to_term(EncodedArgs))
        catch Error:Type:Stacktrace  ->
            ?error_stacktrace(
                "Error during execution of restart posthook ~tp: ~tp:~tp", [Identifier, Error, Type], Stacktrace
            )
        end
    end, RestartHooks),

    ok = datastore_model:delete(?CTX, ?DOC_KEY).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {hooks, #{binary => {record, [
            {module, atom},
            {function, atom},
            {encoded_args, binary}
        ]}}}
    ]}.