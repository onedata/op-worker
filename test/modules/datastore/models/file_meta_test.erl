%%%--------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests for file_meta datastore model.
%%% @end
%%%--------------------------------------------------------------------
-module(file_meta_test).
-author("Konrad Zemek").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("eunit/include/eunit.hrl").

get_name_test() ->
    meck:new(file_meta, [passthrough]),
    meck:expect(file_meta, get,
        fun(entry) -> {ok, #document{value = #file_meta{name = <<"A Name">>}}} end),

    ?assertEqual({ok, <<"A Name">>}, file_meta:get_name(entry)),

    meck:unload(file_meta).
