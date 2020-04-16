%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding files' blocks.
%%% @end
%%%-------------------------------------------------------------------
-module(file_local_blocks).
-author("Michał Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("global_definitions.hrl").

% API
-export([update/2, get/1, delete/1]).
-export([save_local_blocks/2, get_local_blocks/1, delete_local_blocks/2]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1]).

-type id() :: datastore:key().
-type record() :: #file_local_blocks{}.
-type doc() :: datastore_doc:doc(record()).
-type one_or_many(Type) :: Type | [Type].

-export_type([id/0, doc/0]).

-define(CTX, #{
    model => ?MODULE,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

-define(MAX_DOC_SIZE,
    application:get_env(?APP_NAME, blocks_doc_max_size, 50000)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv update(Key, Blocks, 0).
%% @end
%%--------------------------------------------------------------------
-spec update(id(), fslogic_blocks:blocks()) -> ok | {error, term()}.
update(Key, Blocks) ->
    update(Key, Blocks, 0).

%%--------------------------------------------------------------------
%% @doc
%% @equiv get(Key, 0).
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, fslogic_blocks:blocks()} | {error, term()}.
get(Key) ->
    get(Key, 0).

%%--------------------------------------------------------------------
%% @doc
%% @equiv delete(Key, 0).
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    delete(Key, 0).

%%--------------------------------------------------------------------
%% @doc
%% Saves local blocks as links.
%% @end
%%--------------------------------------------------------------------
-spec save_local_blocks(id(), fslogic_blocks:blocks()) ->
    ok | one_or_many({ok, datastore:link()} | {error, term()}).
save_local_blocks(_Key, []) ->
    ok;
save_local_blocks(Key, Blocks) ->
    Links = lists:map(fun(#file_block{offset = O, size = S}) -> {O, S} end, Blocks),
    TreeId = oneprovider:get_id(),
    datastore_model:add_links(?CTX, Key, TreeId, Links).

%%--------------------------------------------------------------------
%% @doc
%% Deletes local blocks' links.
%% @end
%%--------------------------------------------------------------------
-spec delete_local_blocks(id(), fslogic_blocks:blocks() | all) ->
    one_or_many(ok | {error, term()}).
delete_local_blocks(_Key, []) ->
    ok;
delete_local_blocks(Key, all) ->
    TreeId = oneprovider:get_id(),
    Ctx = ?CTX,
    FoldAns = datastore_model:fold_links(Ctx, Key, TreeId, fun
        (#link{name = O}, Acc) ->
            {ok, {[O | Acc]}}
    end, [], #{}),
    case FoldAns of
        {ok, Links} ->
            datastore_model:delete_links(Ctx, Key, TreeId, Links);
        {error, Reason} ->
            {error, Reason}
    end;
delete_local_blocks(Key, Blocks) ->
    TreeId = oneprovider:get_id(),
    Links = lists:map(fun(#file_block{offset = O}) -> O end, Blocks),
    datastore_model:delete_links(?CTX, Key, TreeId, Links).

%%--------------------------------------------------------------------
%% @doc
%% Gets local blocks from links.
%% @end
%%--------------------------------------------------------------------
-spec get_local_blocks(id()) -> {ok, datastore:fold_acc()} | {error, term()}.
get_local_blocks(Key) ->
    TreeId = oneprovider:get_id(),
    FoldAns = datastore_model:fold_links(?CTX, Key, TreeId, fun
        (#link{name = O, target = S}, Acc) ->
            {ok, [#file_block{offset = O, size = S} | Acc]}
    end, [], #{}),
    case FoldAns of
        {ok, Links} ->
            {ok, lists:reverse(Links)};
        Error ->
            Error
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
        {last, boolean},
        {blocks, [term]}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates blocks in doc.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), fslogic_blocks:blocks(), non_neg_integer()) ->
    ok | {error, term()}.
update(Key, Blocks, Num) ->
    MaxSize = ?MAX_DOC_SIZE,
    {{Blocks1, Blocks2}, Last} = case length(Blocks) > MaxSize of
        true -> {lists:split(MaxSize, Blocks), false};
        _ -> {{Blocks, []}, true}
    end,

    DocKey = get_doc_key(Key, Num),
    Diff = fun
        (_) ->
            {ok, #file_local_blocks{
                blocks = Blocks1, last = Last
            }}
    end,
    Default = #document{key = DocKey, value = #file_local_blocks{
        blocks = Blocks1, last = Last
    }},

    case {datastore_model:update(?CTX, DocKey, Diff, Default), Last} of
        {{ok, _}, true} -> ok;
        {{ok, _}, _} -> update(Key, Blocks2, Num + 1);
        {Error, _} -> Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns blocks from doc.
%% @end
%%--------------------------------------------------------------------
-spec get(id(), non_neg_integer()) ->
    {ok, fslogic_blocks:blocks()} | {error, term()}.
get(Key, Num) ->
    case datastore_model:get(?CTX, get_doc_key(Key, Num)) of
        {ok, #document{value = #file_local_blocks{
            blocks = Blocks, last = Last
        }}} ->
            case Last of
                true ->
                    {ok, Blocks};
                _ ->
                    case get(Key, Num + 1) of
                        {ok, Blocks2} -> {ok, Blocks ++ Blocks2};
                        Error -> Error
                    end
            end;
        {error, not_found} ->
            {ok, []};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes docs with blocks.
%% @end
%%--------------------------------------------------------------------
-spec delete(id(), non_neg_integer()) -> ok | {error, term()}.
delete(Key, Num) ->
    DocKey = get_doc_key(Key, Num),
    case datastore_model:get(?CTX, DocKey) of
        {ok, #document{value = #file_local_blocks{last = Last}}} ->
            Result = datastore_model:delete(?CTX, DocKey),
            case Last of
                true -> Result;
                false -> delete(Key, Num + 1)
            end;
        {error, not_found} ->
            ok
    end.

-spec get_doc_key(id(), non_neg_integer()) -> id().
get_doc_key(Key, Num) ->
    datastore_key:build_adjacent(<<Num/integer>>, Key).
