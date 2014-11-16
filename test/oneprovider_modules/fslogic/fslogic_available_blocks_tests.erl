%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests functionality of available blocks module, which helps
%% providers to check if their files are in sync
%% @end
%% ===================================================================
-module(fslogic_available_blocks_tests).

-include_lib("eunit/include/eunit.hrl").
-include("oneprovider_modules/fslogic/fslogic_available_blocks.hrl").

byte_to_block_range_test() ->
    ?assertEqual(#block_range{from = 0, to = 1}, fslogic_available_blocks:byte_to_block_range(#byte_range{from = 123, to = ?remote_block_size + 1})),
    ?assertEqual(#block_range{from = 1, to = 1}, fslogic_available_blocks:byte_to_block_range(#byte_range{from = ?remote_block_size, to = ?remote_block_size})),
    ?assertEqual(#block_range{from = 0, to = 2}, fslogic_available_blocks:byte_to_block_range(#byte_range{from = 0, to = 2*?remote_block_size})).
