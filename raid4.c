/*
 * RAID4, 3 <= # disks <= 16
 * Block Striping with a dedicated parity drive
 *
 * built on
 * RAID1 example for BUSE
 * by Tyler Bletsch to ECE566, Duke University, Fall 2019
 *
 * Based on 'busexmp' by Adam Cozzette
 *
 * This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <argp.h>
#include <assert.h>
#include <err.h>
#include <fcntl.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "buse.h"

#define UNUSED(x)                                                              \
  (void)(x) // used to suppress "unused variable" warnings without turning off
            // the feature entirely

int dev_fd[16]; // max. number of allowed devices
u_int32_t block_size; // NOTE: other than truncating the resulting raid device,
                // block_size is ignored in this program; it is asked for and
                // set in order to make it easier to adapt this code to
                // RAID0/4/5/6.
uint64_t raid_device_size; // size of raid device in bytes
bool verbose = false;      // set to true by -v option for debug output
bool degraded = false;     // true if we're missing a device

int missing_dev =
    -1; // index of dev_fd that has a valid drive (used in degraded
        // mode to identify the non-missing drive (0 or 1))
int rebuild_dev =
    -1; // index of drive that is being added with '+' for RAID rebuilt
int num_devices = 0;   // # devices to stripe over, excludes parity device
int parity_device = 0; // The device after num_devices which contains parity

// function to calculate bitwise xor of two blocks of block_size
static void xor_func(char *result, const char *buf1, const char *buf2) {
  for (u_int32_t i = 0; i < block_size; i++) {
    result[i] = buf1[i] ^ buf2[i];
  }
}

static void degraded_read(char *buf, off_t offset, u_int32_t len) {
  // read all devices except missing device
  // return result of xor in buf
  //  fprintf(stderr, "Degraded Read\n"); // remove
  char readblock[len];
  char result[len];
  memset(result, 0, len);
  for (int i = 0; i <= num_devices; i++) {
    if (i != missing_dev) {
      pread(dev_fd[i], readblock, len, offset);
      for (u_int32_t j = 0; j < len; j++) {
        result[j] ^= readblock[j];
      }
    }
  }
  memcpy(buf, result, block_size);
}

static int xmp_read(void *buf, u_int32_t len, u_int64_t offset,
                    void *userdata) {
  UNUSED(userdata);
  if (verbose)
    fprintf(stderr, "R - %lu, %u\n", offset, len);

  // choose read device based on block number
  int read_device;
  off_t read_offset;
  u_int32_t read_len = len;

  while (len > 0) {
    read_len = (len < block_size)? len : block_size;
    read_device = (offset / block_size) % num_devices;
    read_offset = ((offset / block_size) / num_devices) * block_size +
                  (offset % block_size);

    /* read_device = (offset / block_size) % num_devices; */
    /* read_offset = offset / num_devices; */

    if ((degraded) && (read_device == missing_dev)) {
      if (verbose)
	fprintf(stderr, "xmp_read: Degraded read: dev: %d,off: %lu\n",
              read_device, read_offset);
      degraded_read(buf, read_offset, read_len);
    } else {
      if (len < block_size){
	pread(dev_fd[read_device], buf, len, read_offset);
	return 0;
      }
      pread(dev_fd[read_device], buf, block_size, read_offset);
    }
    
    buf += block_size;
    offset += block_size;
    len -= block_size;
  }

  return 0;
}

static int xmp_write(const void *buf, u_int32_t len, u_int64_t offset,
                     void *userdata) {
  UNUSED(userdata);
  if (verbose)
    fprintf(stderr, "W - %lu, %u\n", offset, len);

  // todo: should be parallelized for performance
  int write_device;
  off_t write_offset;

  char *old_data = malloc(block_size);
  char *old_parity = malloc(block_size);
  char *new_parity = malloc(block_size);
  char *new_data = malloc(block_size);
  char *diff = malloc(block_size);

  u_int32_t write_len = 0;

  memset(old_data, 0, block_size);
  memset(old_parity, 0, block_size);
  memset(new_data, 0, block_size);
  memset(new_parity, 0, block_size);
  memset(diff, 0, block_size);

  while (len > 0) {
    write_len = (len < block_size) ? len : block_size;
    write_device = (offset / block_size) % num_devices;
    write_offset = ((offset / block_size) / num_devices) * block_size +
                   (offset % block_size);
    /* write_device = (offset / block_size) % num_devices; */
    /* write_offset = offset / num_devices; */

    if ((degraded) && (write_device == missing_dev)) {
      // write based on parity
      // call xmp_read for degraded bloc
      if (verbose)
	fprintf(stderr, "xmp_write: Degraded Read\n"); // remove
      memcpy(new_data, buf, block_size);
      degraded_read(old_data, write_offset, write_len);
      pread(dev_fd[parity_device], old_parity, write_len, write_offset);
      
      for (u_int32_t i = 0; i < write_len; i++) {
        new_parity[i] = (old_data[i] ^ new_data[i]) ^ old_parity[i];
      }
      // skip pwrite to missing device
      pwrite(dev_fd[parity_device], new_parity, write_len, write_offset);
    } else if ((degraded) && (parity_device == missing_dev)) {
      // if missing device is parity device, just do striping and ignore parity
      pwrite(dev_fd[write_device], buf, write_len, write_offset);
    } else {
      /* calculate new parity and update */
      memcpy(new_data, buf, block_size);
      pread(dev_fd[write_device], old_data, write_len, write_offset);
      pread(dev_fd[parity_device], old_parity, write_len, write_offset);
      /* xor_func(diff, old_data, new_data); */
      /* xor_func(new_parity, diff, old_parity); */
      for (u_int32_t i = 0; i < write_len; i++) {
        new_parity[i] = (old_data[i] ^ new_data[i]) ^ old_parity[i];
      }
      if (verbose)
	fprintf(stderr, "new-parity: %s\n", new_parity); // remove
      // write new block at write_offset to write_device
      pwrite(dev_fd[write_device], new_data, write_len,
             write_offset); // buf -> new_data
      // write new block at write_offset to parity_device
      if (verbose)
	fprintf(stderr, "parity_device: %d: %d\n", parity_device,
              dev_fd[parity_device]); // remove
      pwrite(dev_fd[parity_device], new_parity, write_len, write_offset);
    }

    if (len < block_size){
      break;
    }
    
    buf += block_size;
    offset += block_size;
    len -= block_size;

    memset(old_data, 0, block_size);
    memset(old_parity, 0, block_size);
    memset(new_data, 0, block_size);
    memset(new_parity, 0, block_size);
    memset(diff, 0, block_size);
  }
  free(old_data);
  free(old_parity);
  free(new_data);
  free(new_parity);
  free(diff);

  return 0;
}

static int xmp_flush(void *userdata) {
  UNUSED(userdata);
  if (verbose)
    fprintf(stderr, "Received a flush request.\n");
  for (int i = 0; i < num_devices; i++) {
    if (dev_fd[i] != -1) { // handle degraded mode
      fsync(dev_fd[i]);    // we use fsync to flush OS buffers to underlying
                           // devices
    }
  }
  return 0;
}

static void xmp_disc(void *userdata) {
  UNUSED(userdata);
  if (verbose)
    fprintf(stderr, "Received a disconnect request.\n");
  // disconnect is a no-op for us
}

/*
// we'll disable trim support, you can add it back if you want it
static int xmp_trim(u_int64_t from, u_int32_t len, void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "T - %lu, %u\n", from, len);
    // trim is a no-op for us
    return 0;
}
*/

/* argument parsing using argp */

static struct argp_option options[] = {
    {"verbose", 'v', 0, 0, "Produce verbose output", 0},
    {0},
};

struct arguments {
  uint32_t block_size;
  char *device[16]; // max num of drives allowed
  char *raid_device;
  int verbose;
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  struct arguments *arguments = state->input;
  char *endptr;

  switch (key) {

  case 'v':
    arguments->verbose = 1;
    break;

  case ARGP_KEY_ARG:
    switch (state->arg_num) {

    case 0:
      arguments->block_size = strtoul(arg, &endptr, 10);
      if (*endptr != '\0') {
        /* failed to parse integer */
        errx(EXIT_FAILURE, "SIZE must be an integer");
      }
      break;

    case 1:
      arguments->raid_device = arg;
      break;
    }

    /* fprintf(stderr, "argparse: key= %d\nstate->arg_num= %d\n", key, */
    /*         state->arg_num); // remove */

    if (state->arg_num > 1) {

      //      fprintf(stderr, "arg= %s\n", arg); // remove

      arguments->device[state->arg_num - 2] = arg;
      /* fprintf(stderr, "arg->device[%d] = %s\n", state->arg_num, */
      /*         arguments->device[state->arg_num]); // remove */
    }
    break;

  case ARGP_KEY_END:
    if ((state->arg_num < 5) ||
        (state->arg_num > 18)) { // block size|raid device|3 block devices|
      warnx("Invalid number of arguments. Hint: Please enter 3 to 16 devices.");
      argp_usage(state);
    }
    num_devices = state->arg_num - 2 - 1; // -bs, -raid device, -parity device
    parity_device = num_devices;
    break;

  default:
    return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

static struct argp argp = {
    .options = options,
    .parser = parse_opt,
    .args_doc = "BLOCKSIZE RAIDDEVICE DEVICE1 DEVICE2",
    .doc = "BUSE implementation of RAID1 for two devices.\n"
           "`BLOCKSIZE` is an integer number of bytes. "
           "\n\n"
           "`RAIDDEVICE` is a path to an NBD block device, for example "
           "\"/dev/nbd0\"."
           "\n\n"
           "`DEVICE*` is a path to underlying block devices. Normal files can "
           "be used too. A `DEVICE` may be specified as \"MISSING\" to run in "
           "degraded mode. "
           "\n\n"
           "If you prepend '+' to a DEVICE, you are re-adding it as a "
           "replacement to the RAID, and we will rebuild the array. "
           "This is synchronous; the rebuild will have to finish before the "
           "RAID is started. "
           "\n\n"
           "Enter three to sixteen block devices."};

static int do_raid_rebuild() {
  // target drive index is: rebuild_dev

  // reconstruction process
  // read all devices except rebuild_dev and xor them
  // save result to rebuild device

  /* int source_dev = (rebuild_dev + 1) % num_devices; // the other one */
  char buf[block_size];
  char readblock[block_size];
  for (int i = 0; i <= num_devices; i++) {
    lseek(dev_fd[i], 0, SEEK_SET);
  }
  for (uint64_t cursor = 0; cursor < raid_device_size; cursor += block_size) {
    // read every device till block_size
    memset(buf, 0, block_size);
    memset(readblock, 0, block_size);
    for (int i = 0; i <= num_devices; i++) {
      if (i != rebuild_dev) {
        read(dev_fd[i], readblock, block_size);
        // xor
        for (u_int32_t j = 0; j < block_size; j++) {
          buf[j] ^= readblock[j];
        }
        // xor
      }
    }
    // buf now contains data to write into rebuild_dev
    write(dev_fd[rebuild_dev], buf, block_size);
  }
  return 0;
}

int main(int argc, char *argv[]) {
  struct arguments arguments = {
      .verbose = 0,
  };
  argp_parse(&argp, argc, argv, 0, 0, &arguments);

  //  fprintf(stderr, "Completed arg parse\n"); // remove

  struct buse_operations bop = {
      .read = xmp_read,
      .write = xmp_write,
      .disc = xmp_disc,
      .flush = xmp_flush,
      // .trim = xmp_trim, // we'll disable trim support, you can add it back
      // if you want it
  };

  //  fprintf(stderr, "Completed buse operations\n"); // remove

  verbose = arguments.verbose;
  block_size = arguments.block_size;

  raid_device_size = 0; // will be detected from the drives available
  missing_dev = -1;

  bool rebuild_needed = false; // will be set to true if a drive is MISSING
  for (int i = 0; i <= num_devices; i++) {
    char *dev_path = arguments.device[i];
    //    fprintf(stderr, "i: %d, dev_path: %s\n", i, dev_path); // remove
    if (strcmp(dev_path, "MISSING") == 0) {
      if (degraded) {
        fprintf(stderr, "ERROR: Multiple MISSING drives specified. RAID4 can "
                        "only tolerate one drive failure.");
        exit(1);
      }
      degraded = true;
      dev_fd[i] = -1;
      missing_dev = i;
      fprintf(stderr, "DEGRADED: Device number %d is missing!\n", i);
    } else {
      if (dev_path[0] == '+') { // RAID rebuild mode!!
        if (rebuild_needed) {
          // multiple +drives detected
          fprintf(stderr, "ERROR: Multiple '+' drives specified. Can only "
                          "recover one drive at a time.\n");
          exit(1);
        }
        dev_path++; // shave off the '+' for the subsequent logic
        rebuild_dev = i;
        rebuild_needed = true;
      }
      dev_fd[i] = open(dev_path, O_RDWR);
      if (dev_fd[i] < 0) {
        perror(dev_path);
        exit(1);
      }
      uint64_t size =
          lseek(dev_fd[i], 0,
                SEEK_END); // used to find device size by seeking to end
      fprintf(stderr, "Got device '%s', size %ld bytes.\n", dev_path, size);
      if (raid_device_size == 0 || size < raid_device_size) {
        raid_device_size =
            size; // raid_device_size is minimum size of available devices
      }
    }
  }

  //  fprintf(stderr, "Completed arg parse\n"); // remove

  raid_device_size = raid_device_size / block_size *
                     block_size; // divide+mult to truncate to block size
  bop.size = raid_device_size;   // tell BUSE how big our block device is
  if (rebuild_needed) {
    if (degraded) {
      fprintf(stderr, "ERROR: Can't rebuild from a missing device (i.e., you "
                      "can't combine MISSING and '+').\n");
      exit(1);
    }
    fprintf(stderr, "Doing RAID rebuild...\n");
    if (do_raid_rebuild() != 0) {
      // error on rebuild
      fprintf(stderr, "Rebuild failed, aborting.\n");
      exit(1);
    }
  }

  fprintf(stderr, "RAID device resulting size: %ld.\n", bop.size);

  return buse_main(arguments.raid_device, &bop, NULL);
}
