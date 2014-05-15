%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic_file.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_file_tests).
-author("Rafal Slota").

-include("veil_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

normalize_file_type_test() ->
    ?assertEqual(?DIR_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?DIR_TYPE)),
    ?assertEqual(?DIR_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?DIR_TYPE_PROT)),
    ?assertEqual(?REG_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?REG_TYPE)),
    ?assertEqual(?REG_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?REG_TYPE_PROT)),
    ?assertEqual(?LNK_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?LNK_TYPE)),
    ?assertEqual(?LNK_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?LNK_TYPE_PROT)),

    ?assertEqual(?DIR_TYPE, fslogic_file:normalize_file_type(internal, ?DIR_TYPE)),
    ?assertEqual(?DIR_TYPE, fslogic_file:normalize_file_type(internal, ?DIR_TYPE_PROT)),
    ?assertEqual(?REG_TYPE, fslogic_file:normalize_file_type(internal, ?REG_TYPE)),
    ?assertEqual(?REG_TYPE, fslogic_file:normalize_file_type(internal, ?REG_TYPE_PROT)),
    ?assertEqual(?LNK_TYPE, fslogic_file:normalize_file_type(internal, ?LNK_TYPE)),
    ?assertEqual(?LNK_TYPE, fslogic_file:normalize_file_type(internal, ?LNK_TYPE_PROT)).


-endif.