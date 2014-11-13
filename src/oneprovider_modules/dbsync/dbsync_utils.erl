%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Utility module for DBSync worker
%% @end
%% ===================================================================
-module(dbsync_utils).
-author("Rafal Slota").

-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

%% API
-export([get_provider_url/1, normalize_seq_info/1, json_decode/1, seq_info_to_url/1, encode_term/1, decode_term/1, changes_json_to_docs/1, gen_request_id/0]).

get_provider_url(ProviderId) ->
    {ok, #provider_details{urls = URLs}} = gr_providers:get_details(provider, ProviderId),
    _URL = lists:nth(crypto:rand_uniform(1, length(URLs) + 1), URLs).


normalize_seq_info(SeqNum) when is_integer(SeqNum) ->
    normalize_seq_info({SeqNum, <<>>});
normalize_seq_info({SeqNum, SeqHash}) when is_integer(SeqNum), is_binary(SeqHash) ->
    {SeqNum, SeqHash};
normalize_seq_info({SeqNum, [SeqHash | _]}) when is_integer(SeqNum), is_binary(SeqHash) ->
    normalize_seq_info({SeqNum, SeqHash});
normalize_seq_info([SeqNum, SeqHash]) ->
    normalize_seq_info({SeqNum, SeqHash}).


json_decode(JSON) ->
    (mochijson2:decoder([{object_hook, fun({struct,L}) -> {L} end}]))(JSON).


seq_info_to_url({SeqNum, SeqHash}) ->
    case SeqNum of
        0 -> "0";
        _ -> "[" ++ integer_to_list(SeqNum) ++ ",\"" ++ binary_to_list(SeqHash) ++ "\"]"
    end.


encode_term(Doc) ->
    term_to_binary(Doc).
decode_term(Doc) ->
    binary_to_term(Doc).


changes_json_to_docs(Data) ->
    {Decoded} = dbsync_utils:json_decode(Data),
    {_, Results} = lists:keyfind(<<"results">>, 1, Decoded),
    {_, [SeqNum, SeqHash]} = lists:keyfind(<<"last_seq">>, 1, Decoded),
    SeqInfo = {SeqNum, SeqHash},
    ChangedDocs = lists:map(
        fun({Result}) ->
            try
                {_, RawDoc} = lists:keyfind(<<"doc">>, 1, Result),
                {_,  [SeqNum1, SeqHash1]} = lists:keyfind(<<"seq">>, 1, Result),
                SeqInfo1 = {SeqNum1, SeqHash1},
                {_,  UUID} = lists:keyfind(<<"id">>, 1, Result),
                {_,  [{[{_, RevBin}]}]} = lists:keyfind(<<"changes">>, 1, Result),
                IsDeleted = proplists:get_value(<<"deleted">>, Result, false),
                [NumBin, HashBin] = binary:split(RevBin, [<<"-">>]),
                Record = dao_records:doc_to_term(RawDoc),
                {#db_document{deleted = IsDeleted, record = Record, uuid = UUID, rev_info = {binary_to_integer(NumBin), [binary:encode_unsigned(erlang:list_to_integer(binary:bin_to_list(HashBin), 16))]}}, SeqInfo1}
            catch
                _:Reason ->
                    ?error("Cannot decode 'changes' JSON response due to ~p", [Reason]),
                    {error, Reason}
            end
        end, Results),
    {ChangedDocs, SeqInfo}.


gen_request_id() ->
    utils:ensure_binary(dao_helper:gen_uuid()).