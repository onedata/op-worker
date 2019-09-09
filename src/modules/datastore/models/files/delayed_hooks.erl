%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model holds information about hooks registered for given file.
%%% All hooks will be executed once for next change of given file's file_meta
%%% document, then hooks list will be cleared.
%%% Any exported function can be used as a hook.
%%% @end
%%%-------------------------------------------------------------------
-module(delayed_hooks).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% functions operating on record using datastore model API
-export([execute_hooks/1, delete/1, add_hook/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).


-type hook() :: #hook{}.
-type hook_identifier() :: binary().

-export_type([hook/0, hook_identifier/0]).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Registers new hook for given file.
%% @end
%%--------------------------------------------------------------------
-spec add_hook(file_meta:uuid(), hook_identifier(), hook()) -> {ok, file_meta:uuid()} | {error, term()}.
add_hook(FileUuid, Identifier, #hook{args = Args} = Hook) ->
    EncodedHook = Hook#hook{args = term_to_binary(Args)},
    datastore_model:update(?CTX, FileUuid, fun(#delayed_hooks{hooks = Hooks} = DelayedHooks) ->
        {ok, DelayedHooks#delayed_hooks{hooks = Hooks#{Identifier => EncodedHook}}}
    end, #delayed_hooks{hooks = #{Identifier => EncodedHook}}).

%%--------------------------------------------------------------------
%% @doc
%% Executes all hooks registered for given file.
%% @end
%%--------------------------------------------------------------------
-spec execute_hooks(datastore:doc()) -> ok.
execute_hooks(#document{key = Key}) ->
    Hooks = case datastore_model:get(?CTX, Key) of
        {ok, #document{value = #delayed_hooks{hooks = H}}} -> H;
        _ -> #{}
    end,
    lists:foreach(fun(#hook{module = Module, function = Function, args = Args}) ->
        try
            ok = erlang:apply(Module, Function, binary_to_term(Args))
        catch Error:Type  ->
            ?debug_stacktrace("Error during execution of delayed hook for file ~p ~p:~p", [Key, Error,Type]),
            ok
        end
    end, maps:values(Hooks)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes document from datastore.
%% @end
%%--------------------------------------------------------------------
-spec delete(file_meta:uuid()) -> ok | {error, term()}.
delete(Key) ->
    case datastore_model:delete(?CTX, Key) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} = Error -> Error
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {hooks, #{binary => {record, [
            {module, atom},
            {function, atom},
            {args, binary}
        ]}}}
    ]}.

