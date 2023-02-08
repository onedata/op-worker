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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% functions operating on record using datastore model API
-export([add_hook/5, execute_hooks/2, cleanup/1]).

%% deprecated functions
-export([execute_hooks_deprecated/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type hook_type() :: doc | link.
-type missing_element() :: {file_meta_missing, MissingUuid :: file_meta:uuid()} |
    {link_missing, Uuid :: file_meta:uuid(), MissingName :: file_meta:name()}.
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

%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================

-spec add_hook(missing_element(), hook_identifier(), module(), atom(), [term()]) ->
    ok | ?ERROR_INTERNAL_SERVER_ERROR.
add_hook(MissingElement, Identifier, Module, Function, PosthookArgs) ->
    FileUuid = get_hook_uuid(MissingElement),
    HookType = missing_element_to_hook_type(MissingElement),
    Key = gen_datastore_key(FileUuid, HookType),
    
    EncodedArgs = Module:encode_file_meta_posthook_args(Function, PosthookArgs),
    
    byte_size(EncodedArgs) =< ?MAX_ENCODED_ARGS_SIZE orelse
        error({file_meta_posthooks_too_large_args, Key, Identifier, byte_size(EncodedArgs)}),
    
    case add_links(Key, {Identifier, encode_hook(Module, Function, EncodedArgs)}) of
        {ok, _} ->
            % Check race with file_meta document and links synchronization. Hook is added when something fails because
            % of missing file_meta document or link. If missing element appears before hook adding to datastore,
            % execution of hook is not triggered by dbsync. Thus, check if missing element exists and trigger hook
            % execution if it exists.
            case has_missing_element_appeared(MissingElement) of
                true ->
                    % Spawn to prevent deadlocks when hook is added from the inside of already existing hook
                    spawn(fun() ->
                        case execute_hook(Key, Identifier, Module, Function, EncodedArgs) of
                            ok -> delete_links(Key, Identifier);
                            error -> ok % hook execution failed, do not remove the hook
                        end
                    end),
                    ok;
                false ->
                    ok
            end;
        Error ->
            ?error("~p:~p error adding file meta posthook for file ~p (identifier ~p, hook module ~p, hook fun ~p): ~p",
                [?MODULE, ?FUNCTION_NAME, FileUuid, Identifier, Module, Function, Error]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


-spec execute_hooks(file_meta:uuid(), hook_type()) -> ok.
execute_hooks(FileUuid, HookType) ->
    Key = gen_datastore_key(FileUuid, HookType),
    critical_section:run(FileUuid, fun() ->
        execute_hooks_unsafe(Key, #{token => #link_token{}, limit => ?FOLD_LINKS_LIMIT})
    end).


-spec cleanup(file_meta:uuid()) -> ok.
cleanup(FileUuid) ->
    lists:foreach(fun(Key) ->
        cleanup_deprecated(Key),
        cleanup_links(Key, #{token => #link_token{}, limit => ?FOLD_LINKS_LIMIT})
    end, [FileUuid, ?LINK_KEY(FileUuid)]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    catch Error:Type:Stacktrace  ->
        ?debug_stacktrace(
            "Error during execution of file meta posthook (~p) for file ~p ~p:~p",
            [Identifier, Key, Error, Type],
            Stacktrace
        ),
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
    [ModuleBin, FunctionBin, EncodedArgs] = binary:split(base64url:decode(EncodedHook), <<?SEPARATOR>>, [global]),
    {binary_to_existing_atom(ModuleBin), binary_to_existing_atom(FunctionBin), EncodedArgs}.


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
    case file_meta_links:get(Uuid, all, MissingName) of
        {ok, _} -> true;
        {error, _} -> false
    end.


%% @private
-spec missing_element_to_hook_type(missing_element()) -> hook_type().
missing_element_to_hook_type({file_meta_missing, _}) -> doc;
missing_element_to_hook_type({link_missing, _, _}) -> link.


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
-spec fold_links(datastore:key(), datastore:fold_fun(datastore:link()), datastore:fold_opts()) ->
    {ok, datastore:fold_acc()} | {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Key, FoldFun, Opts) ->
    datastore_model:fold_links(?CTX, Key, all, FoldFun, [], Opts).


%%%===================================================================
%%% Deprecated functions
%% @TODO VFS-6767 deprecated, included for upgrade procedure. Remove in next major release after 21.02.*.
%%%===================================================================

-spec execute_hooks_deprecated(file_meta:uuid(), hook_type()) -> ok | {error, term()}.
execute_hooks_deprecated(FileUuid, HookType) ->
    Key = gen_datastore_key(FileUuid, HookType),
    case datastore_model:get(?CTX, Key) of
        {ok, #document{value = #file_meta_posthooks{hooks = Hooks}}} ->
            case maps:size(Hooks) of
                0 ->
                    cleanup_deprecated(Key);
                _ ->
                    execute_hooks_deprecated_internal(Key, Hooks)
            end;
        _ ->
            ok
    end.


%% @private
-spec execute_hooks_deprecated_internal(datastore:key(), hooks()) -> ok | {error, term()}.
execute_hooks_deprecated_internal(Key, HooksToExecute) ->
    FailedHooks = maps:fold(fun(Identifier, #hook{module = Module, function = Function, args = Args}, Acc) ->
        case execute_hook(Key, Identifier, Module, Function, term_to_binary(Args)) of
            ok -> Acc;
            error -> [{Identifier, encode_hook(Module, Function, Args)} | Acc]
        end
    end, [], HooksToExecute),
    
    case FailedHooks of
        [] -> ok;
        _ -> add_links(Key, FailedHooks)
    end,
    cleanup_deprecated(Key).


%% @private
-spec cleanup_deprecated(file_meta:uuid()) -> ok | {error, term()}.
cleanup_deprecated(Key) ->
    case datastore_model:delete(?CTX, Key) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} = Error -> Error
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%% @TODO VFS-6767 deprecated, included for upgrade procedure. Remove in next major release after 21.02.*.
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.

%% @TODO VFS-6767 deprecated, included for upgrade procedure. Remove in next major release after 21.02.*.
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

