%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for cdmi_handler module.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_handler_test).
-author("Piotr Ociepka").
-author("Bartosz Walkowicz").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

get_supported_version_test() ->
  Binary00 = <<"">>,
  Binary01 = <<"  ">>,
  Binary02 = <<" ,">>,
  Binary10 = <<"1.1.1">>,
  Binary11 = <<"aaa,1.1.1 ,aaa">>,
  Binary20 = <<"1.0.2">>,
  Binary21 = <<"   2.3.4   ">>,
  Binary22 = <<",42.0.0,">>,
  Binary23 = <<"bbb,1.0.1,ccc">>,
  Binary30 = <<"1">>,
  Binary31 = <<"1.">>,
  Binary32 = <<"1.2">>,
  Binary33 = <<"1.2.">>,
  Binary34 = <<"1.2.3.">>,
  Binary35 = <<"1.2.3.4">>,
  Binary40 = <<"aaaaa">>,
  Binary41 = <<"aaa,aa,">>,
  Binary42 = <<"aaa,aa ">>,

  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary00))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary01))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary02))),

  ?assertEqual(<<"1.1.1">>, cdmi_handler:get_supported_version(Binary10)),
  ?assertEqual(<<"1.1.1">>, cdmi_handler:get_supported_version(Binary11)),

  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary20))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary21))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary22))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary23))),

  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary30))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary31))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary32))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary33))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary34))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary35))),

  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary40))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary41))),
  ?assertEqual(?ERR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>]), (catch cdmi_handler:get_supported_version(Binary42))).

-endif.
