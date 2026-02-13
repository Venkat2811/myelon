use disruptor_mp::{SharedCursor, SharedMemoryConfig, SharedRingBuffer};
use std::sync::atomic::Ordering;

fn unique_name(prefix: &str) -> String {
    format!("{}_{}", prefix, std::process::id())
}

#[test]
fn dropping_attached_cursor_must_not_disrupt_owner() {
    let name = unique_name("sc_attach_drop");
    let owner = SharedCursor::new(&name, 1).expect("owner cursor create should succeed");

    {
        let attached = SharedCursor::attach(&name).expect("attach should succeed");
        attached.store(55, Ordering::Release);
    }

    assert_eq!(
        owner.load(Ordering::Acquire),
        55,
        "owner cursor state should remain valid after attacher drop"
    );

    owner.store(66, Ordering::Release);
    let reattached =
        SharedCursor::attach(&name).expect("reattach should succeed after attacher drop");
    assert_eq!(reattached.load(Ordering::Acquire), 66);
}

#[test]
fn dropping_attached_ringbuffer_must_not_disrupt_owner() {
    let name = unique_name("srb_attach_drop");

    let owner_config = SharedMemoryConfig {
        name: name.clone(),
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: true,
    };

    let owner =
        SharedRingBuffer::<u64>::new(owner_config, || 0).expect("owner ring create should succeed");

    unsafe {
        *owner.get(0) = 77;
    }

    {
        let attach_config = SharedMemoryConfig {
            name: name.clone(),
            buffer_size: 8,
            element_size: std::mem::size_of::<u64>(),
            create: false,
        };

        let attached =
            SharedRingBuffer::<u64>::attach(attach_config).expect("attach should succeed");
        let seen = unsafe { *attached.get(0) };
        assert_eq!(seen, 77);
    }

    unsafe {
        *owner.get(0) = 88;
    }

    let reattach_config = SharedMemoryConfig {
        name,
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: false,
    };
    let reattached = SharedRingBuffer::<u64>::attach(reattach_config)
        .expect("reattach should succeed after attacher drop");

    let seen = unsafe { *reattached.get(0) };
    assert_eq!(seen, 88);
}

#[test]
fn dropping_owner_cursor_releases_name_for_recreate() {
    let name = unique_name("sc_owner_drop");
    {
        let owner = SharedCursor::new(&name, 10).expect("owner cursor create should succeed");
        owner.store(22, Ordering::Release);
    }

    // If cleanup ownership is broken, this create may fail due stale segment.
    let recreated = SharedCursor::new(&name, 33).expect("owner recreate should succeed");
    assert_eq!(recreated.load(Ordering::Acquire), 33);
}

#[test]
fn dropping_owner_ringbuffer_releases_name_for_recreate() {
    let name = unique_name("srb_owner_drop");

    {
        let owner_config = SharedMemoryConfig {
            name: name.clone(),
            buffer_size: 8,
            element_size: std::mem::size_of::<u64>(),
            create: true,
        };
        let owner = SharedRingBuffer::<u64>::new(owner_config, || 0)
            .expect("owner ring create should succeed");
        unsafe {
            *owner.get(0) = 11;
        }
    }

    // If cleanup ownership is broken, this create may fail due stale segment.
    let recreate_config = SharedMemoryConfig {
        name: name.clone(),
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: true,
    };
    let recreated =
        SharedRingBuffer::<u64>::new(recreate_config, || 0).expect("owner recreate should succeed");

    let attach_config = SharedMemoryConfig {
        name,
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: false,
    };
    let attached = SharedRingBuffer::<u64>::attach(attach_config).expect("attach should succeed");

    unsafe {
        *recreated.get(0) = 44;
        assert_eq!(*attached.get(0), 44);
    }
}

#[test]
fn short_name_repeated_create_attach_drop_stress() {
    // Keep names short for macOS NAME_MAX constraints while still unique per process.
    let pid = std::process::id() % 10_000;
    let base = format!("mp{:04}", pid); // <= 8 chars

    for i in 0..200 {
        let name = format!("{base}{:02}", i % 100); // <= 10 chars

        let owner = SharedCursor::new(&name, i).expect("owner create should succeed");
        {
            let attached = SharedCursor::attach(&name).expect("attach should succeed");
            assert_eq!(attached.load(Ordering::Acquire), i);
            attached.store(i + 1, Ordering::Release);
        }
        assert_eq!(owner.load(Ordering::Acquire), i + 1);

        drop(owner);

        // Name must be reusable after owner drop.
        let recreated = SharedCursor::new(&name, i + 2).expect("recreate should succeed");
        assert_eq!(recreated.load(Ordering::Acquire), i + 2);
    }
}
