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
%%% Note: hooks are only triggered for directories. There are
%%% no hardlinks to directories. When hooks on regular files are
%%% introduced, consider usage of referenced uuid.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_posthooks).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% functions operating on record using datastore model API
-export([add_hook/5, execute_hooks/1, delete/1]).

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
-type hooks() :: #{hook_identifier() => hook()}.

-export_type([hooks/0]).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================

-spec add_hook(file_meta:uuid(), hook_identifier(), module(), atom(), [term()]) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
add_hook(FileUuid, Identifier, Module, Function, Args) ->
    UniqueIdentifier = generate_hook_id(Identifier),
    EncodedArgs = term_to_binary(Args),
    Hook = #hook{
        module = Module,
        function = Function,
        args = EncodedArgs
    },

    AddAns = datastore_model:update(?CTX, FileUuid, fun(#file_meta_posthooks{hooks = Hooks} = FileMetaPosthooks) ->
        {ok, FileMetaPosthooks#file_meta_posthooks{hooks = Hooks#{UniqueIdentifier => Hook}}}
    end, #file_meta_posthooks{hooks = #{UniqueIdentifier => Hook}}),

    case AddAns of
        {ok, _} ->
            % Check race with file_meta document synchronization. Hook is added when something fails because of
            % missing file_meta document. If missing file_meta document appears before hook adding to datastore,
            % execution of hook is not triggered by dbsync. Thus, check if file_meta exists and trigger hook
            % execution if it exists.
            % NOTE: this check does not addresses races with link documents synchronization. If any hook depends
            % on links, this race must be handled here.
            case file_meta:exists(FileUuid) of
                true ->
                    % Spawn to prevent deadlocks when hook is added from the inside of already existing hook
                    spawn(fun() -> execute_hooks(FileUuid) end),
                    ok;
                _ -> ok
            end;
        Error ->
            ?error("~p:~p error for file ~p (identifier ~p, hook module ~p, hook fun ~p, hook args ~p): ~p",
                [?MODULE, ?FUNCTION_NAME, FileUuid, Identifier, Module, Function, Args, Error]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


-spec execute_hooks(file_meta:uuid()) -> ok | {error, term()}.
execute_hooks(FileUuid) ->
    case datastore_model:get(?CTX, FileUuid) of
        {ok, #document{value = #file_meta_posthooks{hooks = Hooks}}} ->
            case maps:size(Hooks) of
                0 ->
                    ok;
                _ ->
                    critical_section:run(FileUuid, fun() ->
                        % Get document once more as hooks might have changed before entering to critical section
                        case datastore_model:get(?CTX, FileUuid) of
                            {ok, #document{value = #file_meta_posthooks{hooks = Hooks}}} ->
                                execute_hooks(FileUuid, Hooks);
                            _ ->
                                ok
                        end
                    end)
            end;
        _ ->
            ok
    end.


%% @private
-spec execute_hooks(file_meta:uuid(), hooks()) -> ok | {error, term()}.
execute_hooks(FileUuid, HooksToExecute) ->
    SuccessfulHooks = maps:fold(fun(Identifier, #hook{module = Module, function = Function, args = Args}, Acc) ->
        try
            ok = erlang:apply(Module, Function, binary_to_term(Args)),
            [Identifier | Acc]
        catch Error:Type:Stacktrace  ->
            ?debug_stacktrace(
                "Error during execution of file meta posthook (~p) for file ~p ~p:~p",
                [Identifier, FileUuid, Error, Type],
                Stacktrace
            ),
            Acc
        end
    end, [], HooksToExecute),

    case SuccessfulHooks of
        [] ->
            ok;
        _ ->
            UpdateFun = fun(#file_meta_posthooks{hooks = PreviousHooks} = FileMetaPosthooks) ->
                {ok, FileMetaPosthooks#file_meta_posthooks{hooks = maps:without(SuccessfulHooks, PreviousHooks)}}
            end,
            case datastore_model:update(?CTX, FileUuid, UpdateFun) of
                {ok, #document{value = #file_meta_posthooks{hooks = Hooks}}} when map_size(Hooks) == 0 ->
                    datastore_model:delete(?CTX, FileUuid, fun(#file_meta_posthooks{hooks = H}) -> map_size(H) == 0 end);
                {ok, _} ->
                    ok;
                {error, _} = Error ->
                    Error
            end
    end.


-spec delete(file_meta:uuid()) -> ok | {error, term()}.
delete(Key) ->
    case datastore_model:delete(?CTX, Key) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generated id MUST be unique, as during hook execution another hook can be created 
%% with exactly the same parameters. Adding such hook will result in it being deleted, 
%% and in result not executed after first hook finishes execution.
%% Prefix is only for diagnostic purposes and is not required for correct working.
%% @end
%%--------------------------------------------------------------------
-spec generate_hook_id(binary()) -> hook_identifier().
generate_hook_id(Prefix) ->
    <<Prefix/binary, "_", (datastore_key:new())/binary>>.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.

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

