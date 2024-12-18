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
%%% Any module using file_meta_posthooks must implement `file_meta_posthooks_behaviour`.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_posthooks).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% functions operating on record using datastore model API
-export([add_hook/6, execute_hooks/2, cleanup/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type hook_type() :: doc | link.
-type missing_element() :: ?MISSING_FILE_META(file_meta:uuid()) |
    ?MISSING_FILE_LINK(file_meta:uuid(), file_meta:name()).
-type hook_identifier() :: binary().
-type function_name() :: atom().
% Posthook args encoded with Module:encode_file_meta_posthook_args/2 function.
% Maximum size of resulting binary is ?MAX_ENCODED_ARGS_SIZE bytes.
% Args are encoded by caller so it is aware of this limitation.
-type encoded_args() :: binary().
-type one_or_many(Type) :: Type | [Type].

-export_type([missing_element/0,function_name/0, encoded_args/0]).

%% @TODO VFS-6767 deprecated, included for upgrade procedure. Remove in next major release after 21.02.*.
-define(CTX, #{
    model => ?MODULE
}).

-record(hook, {
    module :: module(),
    function :: atom(),
    % list of arbitrary args encoded using term_to_binary/1
    args :: binary()
}).
-type hook() :: #hook{}.
-type hooks() :: #{hook_identifier() => hook()}.
-export_type([hooks/0]).

-define(LINK_KEY(FileUuid), <<"link_hooks_", FileUuid/binary>>).

-define(SEPARATOR, "#").
-define(FOLD_LINKS_LIMIT, 1000).
-define(MAX_ENCODED_ARGS_SIZE, 512).

-define(SHOULD_IGNORE_ON_INITIAL_SYNC, op_worker:get_env(ignore_file_meta_posthooks_on_initial_sync, false)).

%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================

-spec add_hook(missing_element(), hook_identifier(), od_space:id(), module(), atom(), [term()]) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
add_hook(MissingElement, Identifier, SpaceId, Module, Function, PosthookArgs) ->
    case ?SHOULD_IGNORE_ON_INITIAL_SYNC andalso dbsync_state:set_initial_sync_repeat(SpaceId) of
        ok ->
            ok;
        _ ->
            add_hook_internal(MissingElement, Identifier, Module, Function, PosthookArgs)
    end.


-spec execute_hooks(file_meta:uuid(), hook_type()) -> ok.
execute_hooks(FileUuid, HookType) ->
    Key = gen_datastore_key(FileUuid, HookType),
    run_in_critical_section(FileUuid, fun() ->
        execute_hooks_unsafe(Key, #{token => #link_token{}})
    end).


-spec cleanup(file_meta:uuid()) -> ok.
cleanup(FileUuid) ->
    lists:foreach(fun(Key) ->
        cleanup_links(Key, #{token => #link_token{}})
    end, [FileUuid, ?LINK_KEY(FileUuid)]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec add_hook_internal(missing_element(), hook_identifier(), module(), atom(), [term()]) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
add_hook_internal(MissingElement, Identifier, Module, Function, PosthookArgs) ->
    FileUuid = get_hook_uuid(MissingElement),
    HookType = missing_element_to_hook_type(MissingElement),
    Key = gen_datastore_key(FileUuid, HookType),

    EncodedArgs = Module:encode_file_meta_posthook_args(Function, PosthookArgs),

    byte_size(EncodedArgs) =< ?MAX_ENCODED_ARGS_SIZE orelse
        error({file_meta_posthooks_too_large_args, Key, Identifier, byte_size(EncodedArgs)}),

    Link = {Identifier, encode_hook(Module, Function, EncodedArgs)},
    case ?extract_ok(?ok_if_exists(add_links(Key, Link))) of
        ok ->
            % Check race with file_meta document and links synchronization. Hook is added when something fails because
            % of missing file_meta document or link. If missing element appears before hook adding to datastore,
            % execution of hook is not triggered by dbsync. Thus, check if missing element exists and trigger hook
            % execution if it exists.
            case has_missing_element_appeared(MissingElement) of
                true ->
                    % Spawn to prevent deadlocks when hook is added from the inside of already existing hook
                    spawn(fun() ->
                        run_in_critical_section(FileUuid, fun() ->
                            case get_link(Key, Identifier) of
                                {ok, _} ->
                                    case execute_hook(Key, Identifier, Module, Function, EncodedArgs) of
                                        ok -> delete_links(Key, Identifier);
                                        error -> ok % hook execution failed, do not remove the hook
                                    end;
                                {error, not_found} ->
                                    ok
                            end
                        end)
                    end),
                    ok;
                false ->
                    ok
            end;
        Error ->
            ?error("~tp:~tp error adding file meta posthook for file ~tp (identifier ~tp, hook module ~tp, hook fun ~tp): ~tp",
                [?MODULE, ?FUNCTION_NAME, FileUuid, Identifier, Module, Function, Error]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


%% @private
-spec execute_hooks_unsafe(datastore:key(), datastore:fold_opts()) -> ok.
execute_hooks_unsafe(Key, Opts) ->
    FoldFun = fun(#link{name = Name, target = Target}, Acc) ->
        {ok, [{Name, Target} | Acc]}
    end,
    {{ok, Links}, NextToken} = fold_links(Key, FoldFun, Opts),
    SuccessfulHooks = lists:filtermap(fun({Identifier, EncodedHook}) ->
        {Module, Function, EncodedArgs} = decode_hook(EncodedHook),
        case execute_hook(Key, Identifier, Module, Function, EncodedArgs) of
            ok -> {true, Identifier};
            error -> false
        end
    end, Links),
    delete_links(Key, SuccessfulHooks),
    case NextToken#link_token.is_last of
        true -> ok;
        false -> execute_hooks_unsafe(Key, Opts#{token => NextToken})
    end.


%% @private
-spec execute_hook(datastore:key(), hook_identifier(), module(), atom(), encoded_args()) -> ok | error.
execute_hook(Key, Identifier, Module, Function, EncodedArgs) ->
    try
        case erlang:apply(Module, Function, Module:decode_file_meta_posthook_args(Function, EncodedArgs)) of
            ok -> ok;
            %% @TODO VFS-10296 - handle not fully synced links in this module
            repeat -> error
        end
    catch Class:Reason:Stacktrace ->
        ?debug_exception(?autoformat(Identifier, Key), Class, Reason, Stacktrace),
        error
    end.


%% @private
-spec cleanup_links(datastore:key(), datastore:fold_opts()) -> ok.
cleanup_links(Key, Opts) ->
    FoldFun = fun(#link{name = Name}, Acc) -> {ok, [Name | Acc]} end,
    {{ok, Links}, NextToken} = fold_links(Key, FoldFun, Opts),
    delete_links(Key, Links),
    case NextToken#link_token.is_last of
        true -> ok;
        false -> cleanup_links(Key, Opts#{token => NextToken})
    end.


%% @private
-spec encode_hook(module(), atom(), encoded_args()) -> binary().
encode_hook(Module, Function, EncodedArgs) ->
    base64url:encode(<<(atom_to_binary(Module))/binary, ?SEPARATOR, (atom_to_binary(Function))/binary, ?SEPARATOR, EncodedArgs/binary>>).


%% @private
-spec decode_hook(binary()) -> {module(), atom(), encoded_args()}.
decode_hook(EncodedHook) ->
    DecodedHook = base64url:decode(EncodedHook),
    [ModuleBin, DecodedHookTail] = binary:split(DecodedHook, <<?SEPARATOR>>),
    [FunctionBin, EncodedArgs] = binary:split(DecodedHookTail, <<?SEPARATOR>>),
    {binary_to_atom(ModuleBin), binary_to_atom(FunctionBin), EncodedArgs}.


%% @private
-spec get_hook_uuid(missing_element()) -> file_meta:uuid().
get_hook_uuid(?MISSING_FILE_META(MissingUuid)) ->
    MissingUuid;
get_hook_uuid(?MISSING_FILE_LINK(Uuid, _Name)) ->
    Uuid.


%% @private
-spec has_missing_element_appeared(missing_element()) -> boolean().
has_missing_element_appeared(?MISSING_FILE_META(MissingUuid)) ->
    file_meta:exists(MissingUuid);
has_missing_element_appeared(?MISSING_FILE_LINK(Uuid, MissingName)) ->
    case file_meta_forest:get(Uuid, all, MissingName) of
        {ok, _} -> true;
        {error, _} -> false
    end.


%% @private
-spec missing_element_to_hook_type(missing_element()) -> hook_type().
missing_element_to_hook_type(?MISSING_FILE_META(_)) -> doc;
missing_element_to_hook_type(?MISSING_FILE_LINK(_, _)) -> link.


%% @private
-spec gen_datastore_key(file_meta:uuid(), doc | link) -> datastore:key().
gen_datastore_key(FileUuid, doc) -> FileUuid;
gen_datastore_key(FileUuid, link) -> ?LINK_KEY(FileUuid).


%% @private
-spec add_links(datastore:key(), one_or_many({datastore:link_name(), datastore:link_target()})) ->
    one_or_many({ok, datastore:link()} | {error, term()}).
add_links(Key, Link) ->
    datastore_model:add_links(?CTX, Key, oneprovider:get_id(), Link).


%% @private
-spec delete_links(datastore:key(), one_or_many(datastore:link_name())) -> one_or_many(ok | {error, term()}).
delete_links(Key, Links) ->
    datastore_model:delete_links(?CTX, Key, oneprovider:get_id(), Links).


%% @private
-spec get_link(datastore:key(), hook_identifier()) -> {ok, [datastore:link()]} | {error, term()}.
get_link(Key, Identifier) ->
    datastore_model:get_links(?CTX, Key, oneprovider:get_id(), Identifier).


%% @private
-spec fold_links(datastore:key(), datastore:fold_fun(datastore:link()), datastore:fold_opts()) ->
    {ok, datastore:fold_acc()} | {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Key, FoldFun, Opts) ->
    datastore_model:fold_links(?CTX, Key, all, FoldFun, [], Opts#{size => ?FOLD_LINKS_LIMIT}).

%% @private
-spec run_in_critical_section(file_meta:uuid(), fun (() -> Result)) -> Result.
run_in_critical_section(FileUuid, Fun) ->
    critical_section:run({?MODULE, FileUuid}, Fun).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.
