%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for cdmi_arg_parser module.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_arg_parser_test).
-author("Piotr Ociepka").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").

-define(version, cdmi_arg_parser:get_supported_version).


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

  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary00)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary01)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary02)),

  ?assertEqual(<<"1.1.1">>, ?version(Binary10)),
  ?assertEqual(<<"1.1.1">>, ?version(Binary11)),

  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary20)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary21)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary22)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary23)),


  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary30)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary31)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary32)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary33)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary34)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary35)),

  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary40)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary41)),
  ?assertThrow(?ERROR_UNSUPPORTED_VERSION, ?version(Binary42)).

-endif.