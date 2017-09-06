/*
 *  Copyright (C) 2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.repl;

import java.io.Closeable;
import java.io.File;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Low-level replication interface, which recives messages in an uninterrupted stream.
 * Applications using this interfaces are responsible for encoding messages such that they can
 * be properly separated. Consider an application which writes these two messages (inside the
 * quotes): {@code ["hello", "world"]}. The messages might be read back as {@code ["hello",
 * "world"]}, {@code ["helloworld"]}, {@code ["he", "llowor", "ld"]}, etc.
 *
 * <p>For ensuring that messages aren't torn in the middle when a new leader is elected,
 * messages must be written into the replicator with properly defined boundaries. When writing
 * {@code ["hello", "world"]}, a leader election can cause the second message to be dropped,
 * and then only {@code ["hello"]} is read. If {@code ["helloworld"]} was written, no tearing
 * of the two words can occur. They might both be read or both be dropped, atomically.
 *
 * @author Brian S O'Neill
 */
public interface StreamReplicator extends Replicator {
    /**
     * Open a replicator instance, creating it if necessary.
     *
     * @throws IllegalArgumentException if misconfigured
     */
    public static StreamReplicator open(ReplicatorConfig config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("No configuration");
        }

        File base = config.mBaseFile;
        if (base == null) {
            throw new IllegalArgumentException("No base file configured");
        }

        long groupToken = config.mGroupToken;
        if (groupToken == 0) {
            throw new IllegalArgumentException("No group token configured");
        }

        SocketAddress localAddress = config.mLocalAddress;
        if (localAddress == null) {
            throw new IllegalArgumentException("No local address configured");
        }

        SocketAddress listenAddress = config.mListenAddress;
        if (listenAddress == null) {
            listenAddress = localAddress;
        }

        Set<SocketAddress> seeds = config.mSeeds;

        if (seeds == null) {
            seeds = Collections.emptySet();
        }

        if (config.mMkdirs) {
            base.getParentFile().mkdirs();
        }

        return Controller.open(new FileStateLog(base), groupToken,
                               new File(base.getPath() + ".group"), 
                               localAddress, listenAddress, config.mLocalRole,
                               seeds, config.mLocalSocket);
    }

    /**
     * Start accepting replication data, to be called for new or existing replicators.
     *
     * @return false if already started
     */
    boolean start() throws IOException;

    /**
     * Start by receiving a {@link #requestSnapshot snapshot} from another group member,
     * expected to be called only by newly joined members.
     *
     * @param options requested options; can pass null if none
     * @return null if no snapshot could be found and replicator hasn't started
     * @throws ConnectException if a snapshot was found, but requesting it failed
     * @throws IllegalStateException if already started
     */
    SnapshotReceiver restore(Map<String, String> options) throws IOException;

    /**
     * Returns a new reader which accesses data starting from the given index. The reader
     * returns EOF whenever the end of a term is reached. At the end of a term, try to obtain a
     * new writer to determine if the local member has become the leader.
     *
     * <p>When passing true for the follow parameter, a reader is always provided at the
     * requested index. When passing false for the follow parameter, null is returned if the
     * current member is the leader for the given index.
     *
     * <p><b>Note: Reader instances are not expected to be thread-safe.</b>
     *
     * @param index index to start reading from, known to have been committed
     * @param follow pass true to obtain an active reader, even if local member is the leader
     * @return reader or possibly null when follow is false
     * @throws IllegalStateException if index is lower than the start index
     */
    Reader newReader(long index, boolean follow);

    /**
     * Returns a new writer for the leader to write into, or else returns null if the local
     * member isn't the leader. The writer stops accepting messages when the term has ended,
     * and possibly another leader has been elected.
     *
     * <p><b>Note: Writer instances are not expected to be thread-safe.</b>
     *
     * @return writer or null if not the leader
     * @throws IllegalStateException if an existing writer for the current term already exists
     */
    Writer newWriter();

    /**
     * Returns a new writer for the leader to write into, or else returns null if the local
     * member isn't the leader. The writer stops accepting messages when the term has ended,
     * and possibly another leader has been elected.
     *
     * <p><b>Note: Writer instances are not expected to be thread-safe.</b>
     *
     * @param index expected index to start writing from as leader; method returns null if
     * index doesn't match
     * @return writer or null if not the leader
     * @throws IllegalArgumentException if given index is negative
     * @throws IllegalStateException if an existing writer for the current term already exists
     */
    Writer newWriter(long index);

    /**
     * Durably persist all data up to the highest index. The highest term, the highest index,
     * and the commit index are all recovered when reopening the replicator. Incomplete data
     * beyond this is discarded.
     */
    void sync() throws IOException;

    /**
     * Called to pass along a control message, which was originally provided through an {@link
     * #controlMessageAcceptor acceptor}. Control messages must be passed along in the original
     * order in which they were created.
     *
     * @param index log index just after the message
     */
    void controlMessageReceived(long index, byte[] message);

    /**
     * Install a callback to be invoked when the replicator needs to send control messages,
     * which must propagate through the replication log. From the perspective of the acceptor,
     * control messages should be treated as opaque. Control messages are primarily used for
     * supporting group membership changes, and without an acceptor, members cannot be added or
     * removed.
     *
     * <p>Acceptor implementations are expected to wrap messages such that they can be
     * propagated along with regular messages, and then later be passed to the {@link
     * #controlMessageReceived controlMessageReceived} method. If a control message cannot be
     * written (possibly because the local member isn't the leader), it might be silently
     * dropped. Implementations are not required to pass control messages to a remote leader.
     *
     * @param acceptor acceptor to use, or pass null to disable
     */
    void controlMessageAcceptor(Consumer<byte[]> acceptor);

    /**
     * Connect to a remote replication group member, for receiving a database snapshot. An
     * {@link #snapshotRequestAcceptor acceptor} must be installed on the group member being
     * connected to for the request to succeed.
     * 
     * <p>The sender is selected as the one which has the fewest count of active snapshot
     * sessions. If all the counts are the same, then a sender is instead randomly selected,
     * favoring a follower over a leader.
     *
     * @param options requested options; can pass null if none
     * @return null if no snapshot could be found
     * @throws ConnectException if a snapshot was found, but requesting it failed
     */
    SnapshotReceiver requestSnapshot(Map<String, String> options) throws IOException;

    /**
     * Install a callback to be invoked when a snapshot is requested by a new group member.
     *
     * @param acceptor acceptor to use, or pass null to disable
     */
    void snapshotRequestAcceptor(Consumer<SnapshotSender> acceptor);

    public static interface Accessor extends Closeable {
        /**
         * Returns the fixed term being accessed.
         */
        long term();

        /**
         * Returns the fixed index at the start of the term.
         */
        long termStartIndex();

        /**
         * Returns the current term end index, which is Long.MAX_VALUE if unbounded. The end
         * index is always permitted to retreat, but never lower than the commit index.
         */
        long termEndIndex();

        /**
         * Returns the next log index which will be accessed.
         */
        long index();

        @Override
        void close();
    }

    public static interface Reader extends Accessor {
        /**
         * Blocks until log messages are available, never reading past a commit index or term.
         *
         * @return amount of bytes read, or EOF (-1) if the term end has been reached
         * @throws IllegalStateException if log was deleted (index is too low)
         */
        default int read(byte[] buf) throws IOException {
            return read(buf, 0, buf.length);
        }

        /**
         * Blocks until log messages are available, never reading past a commit index or term.
         *
         * @return amount of bytes read, or EOF (-1) if the term end has been reached
         * @throws IllegalStateException if log was deleted (index is too low)
         */
        int read(byte[] buf, int offset, int length) throws IOException;
    }

    public static interface Writer extends Accessor {
        /**
         * Write complete messages to the log.
         *
         * @return amount of bytes written, which is less than the message length only if the
         * writer is deactivated
         */
        default int write(byte[] messages) throws IOException {
            return write(messages, 0, messages.length);
        }

        /**
         * Write complete messages to the log.
         *
         * @return amount of bytes written, which is less than the given length only if the
         * writer is deactivated
         */
        default int write(byte[] messages, int offset, int length) throws IOException {
            return write(messages, offset, length, index() + length);
        }

        /**
         * Write complete or partial messages to the log.
         *
         * @param highestIndex highest index (exclusive) which can become the commit index
         * @return amount of bytes written, which is less than the given length only if the
         * writer is deactivated
         */
        int write(byte[] messages, int offset, int length, long highestIndex) throws IOException;

        /**
         * Blocks until the commit index reaches the given index.
         *
         * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
         * @return current commit index, or -1 if deactivated before the index could be
         * reached, or -2 if timed out
         */
        long waitForCommit(long index, long nanosTimeout) throws InterruptedIOException;

        /**
         * Blocks until the commit index reaches the end of the term.
         *
         * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
         * @return current commit index, or -1 if closed before the index could be
         * reached, or -2 if timed out
         */
        default long waitForEndCommit(long nanosTimeout) throws InterruptedIOException {
            long endNanos = nanosTimeout > 0 ? (System.nanoTime() + nanosTimeout) : 0;

            long endIndex = termEndIndex();

            while (true) {
                long index = waitForCommit(endIndex, nanosTimeout);
                if (index == -2) {
                    // Timed out.
                    return -2;
                }
                endIndex = termEndIndex();
                if (endIndex == Long.MAX_VALUE) {
                    // Assume closed.
                    return -1;
                }
                if (index == endIndex) {
                    // End reached.
                    return index;
                }
                // Term ended even lower, so try again.
                if (nanosTimeout > 0) {
                    nanosTimeout = Math.max(0, endNanos - System.nanoTime());
                }
            }
        }

        /**
         * Invokes the given task when the commit index reaches the requested index. The
         * current commit index is passed to the task, or -1 if the term ended before the index
         * could be reached. If the task can be run when this method is called, then the
         * current thread invokes it immediately.
         */
        void uponCommit(long index, LongConsumer task);

        /**
         * Invokes the given task when the commit index reaches the end of the term. The
         * current commit index is passed to the task, or -1 if if closed. If the task can be
         * run when this method is called, then the current thread invokes it immediately.
         */
        default void uponEndCommit(LongConsumer task) {
            uponCommit(termEndIndex(), index -> {
                long endIndex = termEndIndex();
                if (endIndex == Long.MAX_VALUE) {
                    // Assume closed.
                    task.accept(-1);
                } else if (index == endIndex) {
                    // End reached.
                    task.accept(index);
                } else {
                    // Term ended even lower, so try again.                        
                    uponEndCommit(task);
                }
            });
        }
    }
}
