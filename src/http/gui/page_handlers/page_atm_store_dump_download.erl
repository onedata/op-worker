%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when atm store dump download page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_atm_store_dump_download).
-author("Bartosz Walkowicz").

-behaviour(dynamic_page_behaviour).

-include("http/gui_paths.hrl").
-include("http/http_download.hrl").
-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").


-export([gen_dump_download_url/2, handle/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the URL under which given atm store dump can be downloaded.
%% The URL contains a one-time download code.
%% @end
%%--------------------------------------------------------------------
-spec gen_dump_download_url(session:id(), atm_store:id()) -> binary().
gen_dump_download_url(SessionId, AtmStoreId) ->
    {ok, Code} = file_download_code:create(#atm_store_dump_download_args{
        session_id = SessionId,
        store_id = AtmStoreId
    }),

    str_utils:format_bin("https://~s~s/~s", [
        oneprovider:get_domain(),
        ?GUI_ATM_STORE_DUMP_DOWNLOAD_PATH,
        Code
    ]).


%% ====================================================================
%% dynamic_page_behaviour callbacks
%% ====================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"GET">>, Req1) ->
    DownloadCode = cowboy_req:binding(code, Req1),

    case file_download_code:verify(DownloadCode) of
        {true, #atm_store_dump_download_args{session_id = SessionId, store_id = AtmStoreId}} ->
            AtmStoreCtx = ?check(atm_store_api:get_ctx(AtmStoreId)),
            Req2 = atm_store_dump_download_utils:handle_download(Req1, SessionId, AtmStoreCtx),
            file_download_code:remove(DownloadCode),
            Req2;
        _ ->
            http_req:send_error(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), Req1)
    end.
