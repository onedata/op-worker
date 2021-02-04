%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model holds information about hooks registered for given file.
%%% All hooks will be executed once upon the next change of file_meta
%%% document associated with given file, then hooks list will be cleared.
%%% Any exported function can be used as a hook.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_posthooks).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% functions operating on record using datastore model API
-export([execute_hooks/1, delete/1, add_hook/5]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-record(hook, {
    module :: module(),
    function :: atom(),
    % list of arbitrary args encoded using term_to_binary/1
    args :: binary()
}).

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
-spec add_hook(file_meta:uuid(), hook_identifier(), module(), atom(), [term()]) ->
    {ok, file_meta:uuid()} | {error, term()}.
add_hook(FileUuid, Identifier, Module, Function, Args) ->
    EncodedArgs = term_to_binary(Args),
    Hook = #hook{
        module = Module,
        function = Function,
        args = EncodedArgs
    },
    datastore_model:update(?CTX, FileUuid, fun(#file_meta_posthooks{hooks = Hooks} = FileMetaPosthooks) ->
        {ok, FileMetaPosthooks#file_meta_posthooks{hooks = Hooks#{Identifier => Hook}}}
    end, #file_meta_posthooks{hooks = #{Identifier => Hook}}).

%%--------------------------------------------------------------------
%% @doc
%% Executes all hooks registered for given file, then clears hook list.
%% @end
%%--------------------------------------------------------------------
-spec execute_hooks(file_meta:uuid()) -> ok | {error, term()}.
execute_hooks(FileUuid) ->
    Hooks = case datastore_model:get(?CTX, FileUuid) of
        {ok, #document{value = #file_meta_posthooks{hooks = H}}} -> H;
        _ -> #{}
    end,
    maps:fold(fun(Identifier, #hook{module = Module, function = Function, args = Args}, _) ->
        try
            ok = erlang:apply(Module, Function, binary_to_term(Args))
        catch Error:Type  ->
            ?debug_stacktrace(
                "Error during execution of file meta posthook (~p) for file ~p ~p:~p",
                [Identifier, FileUuid, Error, Type]
            ),
            ok
        end
    end, ok, Hooks),

    delete(FileUuid).

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

