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
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/ranges_struct.hrl").

byte_to_block_range_test() ->
    ?assertEqual(#block_range{from = 0, to = 1}, fslogic_available_blocks:byte_to_block_range(#byte_range{from = 0, to = ?remote_block_size})),
    ?assertEqual(#block_range{from = 1, to = 1}, fslogic_available_blocks:byte_to_block_range(#byte_range{from = ?remote_block_size, to = ?remote_block_size})),
    ?assertEqual(#block_range{from = 0, to = 2}, fslogic_available_blocks:byte_to_block_range(#byte_range{from = 0, to = 2*?remote_block_size})).

mark_as_available_test() ->
    Parts = [#range{from = 0, to = 80, timestamp = 10}, #range{from = 101, to = 101, timestamp = 11}, #range{from = 102, to = 102, timestamp = 12}],
    DocRecord = #available_blocks{file_parts = Parts},
    Doc = #db_document{record = #available_blocks{file_parts = Parts}},

    %changes already marked
    ToMark = [#range{from = 6, to = 8, timestamp = 10}, #range{from = 101, to = 101, timestamp = 11}],
    ?assertEqual(Doc, fslogic_available_blocks:mark_as_available(ToMark, Doc)),

    %some new changes
    ToMark2 = [#range{from = 10, to = 12, timestamp = 11}, #range{from = 85, to = 86, timestamp = 11}, #range{from = 110, to = 110, timestamp = 11}],
    PartsAns = [
        #range{from = 0, to = 9, timestamp = 10},
        #range{from = 10, to = 12, timestamp = 11},
        #range{from = 13, to = 80, timestamp = 10},
        #range{from = 85, to = 86, timestamp = 11},
        #range{from = 101, to = 101, timestamp = 11},
        #range{from = 102, to = 102, timestamp = 12},
        #range{from = 110, to = 110, timestamp = 11}
    ],
    DocAns = fslogic_available_blocks:mark_as_available(ToMark2, Doc),
    ?assertEqual(Doc#db_document{record = DocRecord#available_blocks{file_parts = PartsAns}}, DocAns).

mark_other_provider_changes_test() ->
    Parts = [
        #range{from = 0, to = 80, timestamp = 10},
        #range{from = 101, to = 101, timestamp = 11},
        #range{from = 102, to = 102, timestamp = 12}
    ],
    DocRecord = #available_blocks{file_parts = Parts},
    Doc = #db_document{record = #available_blocks{file_parts = Parts}},

    %changes already marked
    ToMark = [#range{from = 6, to = 8, timestamp = 10}, #range{from = 101, to = 101, timestamp = 11}],
    OtherDoc = #db_document{record = #available_blocks{file_parts = ToMark}},
    ?assertEqual(Doc, fslogic_available_blocks:mark_other_provider_changes(Doc, OtherDoc)),

    %some new changes
    ToMark2 = [
        #range{from = 10, to = 12, timestamp = 11},
        #range{from = 85, to = 86, timestamp = 11},
        #range{from = 110, to = 110, timestamp = 11}
    ],
    OtherDoc2 = #db_document{record = #available_blocks{file_parts = ToMark2}},
    PartsAns = [
        #range{from = 0, to = 9, timestamp = 10},
        #range{from = 13, to = 80, timestamp = 10},
        #range{from = 101, to = 101, timestamp = 11},
        #range{from = 102, to = 102, timestamp = 12}
    ],
    DocAns = fslogic_available_blocks:mark_other_provider_changes(Doc, OtherDoc2),
    ?assertEqual(Doc#db_document{record = DocRecord#available_blocks{file_parts = PartsAns}}, DocAns).
