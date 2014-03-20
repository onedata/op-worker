%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides mapping of gui paths to modules that will
%% render the pages. Note that pages using any actions or events
%% must be available both under /page_name and /ws/page_name.
%% @end
%% ===================================================================

-module(gui_routes).
-behaviour(route_handler).
-include_lib("n2o/include/wf.hrl").
-export([init/2, finish/2]).

finish(State, Ctx) -> {ok, State, Ctx}.
init(State, Ctx) ->
    Path = wf:path(Ctx#context.req),
    {ok, State, Ctx#context{path = Path, module = route(Path)}}.

route(<<"/">>) -> page_file_manager;
route(<<"/ws/">>) -> page_file_manager;

route(<<"/login">>) -> page_login;
route(<<"/ws/login">>) -> page_login;

route(<<"/validate_login">>) -> page_validate_login;
route(<<"/ws/validate_login">>) -> page_validate_login;

route(<<"/logout">>) -> page_logout;
route(<<"/ws/logout">>) -> page_logout;

route(<<"/file_manager">>) -> page_file_manager;
route(<<"/ws/file_manager">>) -> page_file_manager;

route(<<"/shared_files">>) -> page_shared_files;
route(<<"/ws/shared_files">>) -> page_shared_files;

route(<<"/logs">>) -> page_logs;
route(<<"/ws/logs">>) -> page_logs;

route(<<"/manage_account">>) -> page_manage_account;
route(<<"/ws/manage_account">>) -> page_manage_account;

route(<<"/about">>) -> page_about;
route(<<"/ws/about">>) -> page_about;

route(<<"/error">>) -> page_error;
route(<<"/ws/error">>) -> page_error;

route(_) -> page_404.
