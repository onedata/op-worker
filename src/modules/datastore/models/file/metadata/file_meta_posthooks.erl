%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model holds information about hooks registered for given file.
%%% All hooks will be executed once upon the next change of required
%%% document associated with given file.
%%% There are 2 types of posthooks, depending on type of missing element:
%%%  * file_meta hooks - hooks, that will be executed, when file_meta document appears;
%%%  * link hooks - hooks, that will be executed, when any link document appears.
%%% After successful hook execution it is removed, when it returns `ok`
%%% and retained when returns `repeat` (useful with link hooks, as it
%%% can be triggered by any link document).
%%% Any exported function can be used as a hook.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_posthooks).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% functions operating on record using datastore model API
-export([add_hook/6, execute_hooks/2, cleanup/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-record(hook, {
    module :: module(),
    function :: atom(),
    % list of arbitrary args encoded using term_to_binary/1
    args :: binary()
}).

-type hook() :: #hook{}.
-type hook_type() :: doc | link.
-type missing_element() :: {file_meta_missing, MissingUuid :: file_meta:uuid()} |
    {link_missing, Uuid :: file_meta:uuid(), MissingName :: file_meta:name()}.
-type hook_identifier() :: binary().
-type hooks() :: #{hook_identifier() => hook()}.

-export_type([hooks/0, missing_element/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(MAX_POSTHOOKS,  op_worker:get_env(max_file_meta_posthooks, 256)).
-define(LINK_KEY(FileUuid), <<"link_hooks_", FileUuid/binary>>).

%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================

-spec add_hook(missing_element(), hook_identifier(), od_space:id(), module(), atom(), [term()]) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
add_hook(MissingElement, Identifier, SpaceId, Module, Function, Args) ->
    FileUuid = get_hook_uuid(MissingElement),
    EncodedArgs = term_to_binary(Args),
    Hook = #hook{
        module = Module,
        function = Function,
        args = EncodedArgs
    },

    HookType = missing_element_to_hook_type(MissingElement),
    AddAns = datastore_model:update(?CTX, gen_datastore_key(FileUuid, HookType), fun(#file_meta_posthooks{hooks = Hooks} = FileMetaPosthooks) ->
        case maps:size(Hooks) >= ?MAX_POSTHOOKS of
            true -> {error, too_many_posthooks};
            false -> {ok, FileMetaPosthooks#file_meta_posthooks{hooks = Hooks#{Identifier => Hook}}}
        end
    end, #file_meta_posthooks{hooks = #{Identifier => Hook}}),

    case AddAns of
        {ok, _} ->
            % Check race with file_meta document and links synchronization. Hook is added when something fails because
            % of missing file_meta document or link. If missing element appears before hook adding to datastore,
            % execution of hook is not triggered by dbsync. Thus, check if missing element exists and trigger hook
            % execution if it exists.
            case has_missing_element_appeared(MissingElement) of
                true ->
                    % Spawn to prevent deadlocks when hook is added from the inside of already existing hook
                    spawn(fun() -> execute_hooks(FileUuid, missing_element_to_hook_type(MissingElement)) end),
                    ok;
                false ->
                    ok
            end;
        {error, too_many_posthooks} ->
            case dbsync_state:set_initial_sync_repeat(SpaceId) of
                ok ->
                    ok;
                {error, initial_sync_does_not_exist} ->
                    ?error("~p:~p error for file ~p (identifier ~p, hook module ~p, hook fun ~p, hook args ~p):"
                        " too many posthooks", [?MODULE, ?FUNCTION_NAME, FileUuid, Identifier, Module, Function, Args])
            end;
        Error ->
            ?error("~p:~p error for file ~p (identifier ~p, hook module ~p, hook fun ~p, hook args ~p): ~p",
                [?MODULE, ?FUNCTION_NAME, FileUuid, Identifier, Module, Function, Args, Error]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


-spec execute_hooks(file_meta:uuid(), hook_type()) -> ok | {error, term()}.
execute_hooks(FileUuid, HookType) ->
    Key = gen_datastore_key(FileUuid, HookType),
    case datastore_model:get(?CTX, Key) of
        {ok, #document{value = #file_meta_posthooks{hooks = Hooks}}} ->
            case maps:size(Hooks) of
                0 ->
                    ok;
                _ ->
                    critical_section:run(FileUuid, fun() ->
                        % Get document once more as hooks might have changed before entering to critical section
                        case datastore_model:get(?CTX, Key) of
                            {ok, #document{value = #file_meta_posthooks{hooks = HooksToExecute}}} ->
                                execute_hooks_unsafe(Key, HooksToExecute);
                            _ ->
                                ok
                        end
                    end)
            end;
        _ ->
            ok
    end.


%% @private
-spec execute_hooks_unsafe(datastore:key(), hooks()) -> ok | {error, term()}.
execute_hooks_unsafe(Key, HooksToExecute) ->
    SuccessfulHooks = maps:fold(fun(Identifier, #hook{module = Module, function = Function, args = Args}, Acc) ->
        try
            case erlang:apply(Module, Function, binary_to_term(Args)) of
                ok -> [Identifier | Acc];
                %% @TODO VFS-10296 - handle not fully synced links in this module
                repeat -> Acc
            end
        catch Error:Type:Stacktrace  ->
            ?debug_stacktrace(
                "Error during execution of file meta posthook (~p) for file ~p ~p:~p",
                [Identifier, Key, Error, Type],
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
            case datastore_model:update(?CTX, Key, UpdateFun) of
                {ok, #document{value = #file_meta_posthooks{hooks = Hooks}}} when map_size(Hooks) == 0 ->
                    case datastore_model:delete(?CTX, Key, fun(#file_meta_posthooks{hooks = H}) -> map_size(H) == 0 end) of
                        ok -> ok;
                        {error, {not_satisfied, _}} -> ok;
                        {error, Reason} -> {error, Reason}
                    end;
                {ok, _} ->
                    ok;
                {error, _} = Error ->
                    Error
            end
    end.


-spec cleanup(file_meta:uuid()) -> ok | {error, term()}.
cleanup(Key) ->
    case datastore_model:delete(?CTX, Key) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} = Error -> Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_hook_uuid(missing_element()) -> file_meta:uuid().
get_hook_uuid({file_meta_missing, MissingUuid}) ->
    MissingUuid;
get_hook_uuid({link_missing, Uuid, _MissingName}) ->
    Uuid.


%% @private
-spec has_missing_element_appeared(missing_element()) -> boolean().
has_missing_element_appeared({file_meta_missing, MissingUuid}) ->
    file_meta:exists(MissingUuid);
has_missing_element_appeared({link_missing, Uuid, MissingName}) ->
    case file_meta_forest:get(Uuid, all, MissingName) of
        {ok, _} -> true;
        {error, _} -> false
    end.


%% @private
-spec missing_element_to_hook_type(missing_element()) -> hook_type().
missing_element_to_hook_type({file_meta_missing, _}) -> doc;
missing_element_to_hook_type({link_missing, _, _}) -> link.


gen_datastore_key(FileUuid, doc) -> FileUuid;
gen_datastore_key(FileUuid, link) -> ?LINK_KEY(FileUuid).

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