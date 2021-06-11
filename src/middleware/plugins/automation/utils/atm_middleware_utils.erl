%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module utility functions for atm_middleware_* plugins.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_middleware_utils).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/errors.hrl").

-export([check_result/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Checks result of lfm atm call and if it's posix error translate id into more
%% human friendly one. Otherwise returns it.
%% @end
%%--------------------------------------------------------------------
-spec check_result(OK | {error, term()}) -> OK | no_return() when
    OK :: ok | {ok, term()} | {ok, term(), term()} | {ok, term(), term(), term()}.
check_result(ok) -> ok;
check_result({ok, _} = Res) -> Res;
check_result({ok, _, _} = Res) -> Res;
check_result({ok, _, _, _} = Res) -> Res;
check_result(?ERROR_NOT_FOUND) -> throw(?ERROR_NOT_FOUND);
check_result({error, ?ENOENT}) -> throw(?ERROR_NOT_FOUND);
check_result({error, ?EACCES}) -> throw(?ERROR_FORBIDDEN);
check_result({error, ?EPERM}) -> throw(?ERROR_FORBIDDEN);
check_result({error, _}) -> throw(?ERROR_MALFORMED_DATA).
