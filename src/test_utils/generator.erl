%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Generates random names.
%%% @end
%%%--------------------------------------------------------------------
-module(generator).
-author("Tomasz Lichon").

-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([gen_name/0, gen_storage_dir/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Generate random storage dir inside ct priv_dir.
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_dir() -> string().
gen_storage_dir() ->
    filename:join([?TEMP_DIR, "storage", erlang:binary_to_list(gen_name())]).

%%--------------------------------------------------------------------
%% @doc
%% Generate random name.
%% @end
%%--------------------------------------------------------------------
-spec gen_name() -> binary().
gen_name() ->
    binary:replace(base64:encode(crypto:strong_rand_bytes(12)), <<"/">>, <<"">>, [global]).

%%%===================================================================
%%% Internal functions
%%%===================================================================