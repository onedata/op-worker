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
%%% Any exported function that accept binaries as arguments can be used as hook.
%%% @end
%%%-------------------------------------------------------------------
-module(delayed_hooks).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").

%% functions operating on record using datastore model API
-export([execute_hooks/1, delete/1, add_hook/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).


-type hook() :: fun((datastore:doc()) -> ok).

-export_type([hook/0]).

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
-spec add_hook(file_meta:uuid(), hook()) -> {ok, file_meta:uuid()} | {error, term()}.
add_hook(FileUuid, Hook) ->
    datastore_model:update(?CTX, FileUuid, fun(#delayed_hooks{hooks = Hooks} = DelayedHooks) ->
        {ok, DelayedHooks#delayed_hooks{hooks = [term_to_binary(Hook) | Hooks]}}
    end, #delayed_hooks{hooks = [term_to_binary(Hook)]}).

%%--------------------------------------------------------------------
%% @doc
%% Executes all hooks registered for given file.
%% @end
%%--------------------------------------------------------------------
-spec execute_hooks(datastore:doc()) -> ok.
execute_hooks(#document{key = Key} = Doc) ->
    Hooks = case datastore_model:get(?CTX, Key) of
        {ok, #document{value = #delayed_hooks{hooks = H}}} -> H;
        _ -> []
    end,
    lists:foreach(fun(EncodedHook) ->
        Hook = binary_to_term(EncodedHook),
        ok = Hook(Doc)
    end, Hooks).

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
        {hooks, [binary]}
    ]}.

