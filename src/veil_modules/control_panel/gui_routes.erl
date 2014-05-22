%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides mapping of gui paths to modules that will
%% render the pages.
%% @end
%% ===================================================================

-module(gui_routes).
-include_lib("n2o/include/wf.hrl").
-export([init/2, finish/2]).

finish(State, Ctx) -> {ok, State, Ctx}.
init(State, Ctx) ->
    Path = wf:path(Ctx#context.req),
    RequestedPage = case Path of
                        <<"/ws", Rest/binary>> -> Rest;
                        Other -> Other
                    end,
    {ok, State, Ctx#context{path = Path, module = route(RequestedPage)}}.

route(<<"/">>) -> page_file_manager;
route(<<"/login">>) -> page_login;
route(<<"/validate_login">>) -> page_validate_login;
route(<<"/logout">>) -> page_logout;
route(<<"/file_manager">>) -> page_file_manager;
route(<<"/shared_files">>) -> page_shared_files;
route(<<"/logs">>) -> page_logs;
route(<<"/manage_account">>) -> page_manage_account;
route(<<"/about">>) -> page_about;
route(<<"/error">>) -> page_error;
route(<<"/test">>) -> page_test;
route(_) -> page_404.
