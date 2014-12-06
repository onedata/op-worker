%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc Provides definitions for available blocks module, which helps
%% providers to check if their files are in sync
%% @end
%% ===================================================================

-include("oneprovider_modules/dao/dao_vfs.hrl").

-ifndef(FSLOGIC_AVAILABLE_BLOCKS_HRL).
-define(FSLOGIC_AVAILABLE_BLOCKS_HRL, 1).

% 'remote_block_size' defines the size of minimal file unit in synchronization process.
% The data that needs to be transfered during file synchronization must be multiple
% of this value in order to be sure that everything is up to date
-define(remote_block_size, 4194304). % 4MB in Bytes

% Range of file value given in bytes ('from' and 'to' are inslusive)
-record(byte_range, {from = 0, to = 0}).

% Range of file value given in offset-size format
-record(offset_range, {offset = 0, size = 0}).

% Range of file value given in remote_blocks ('from' and 'to' are inslusive)
-record(block_range, {from = 0, to = 0}).

-endif.