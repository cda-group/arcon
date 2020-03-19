use crate::{prelude::ArconResult, state_backend::StateBackend};
use ::sled::{open, Db};
use std::{
    fs,
    fs::File,
    io,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

pub struct Sled {
    db: Db,
}

impl StateBackend for Sled {
    fn new(path: &str) -> ArconResult<Self>
    where
        Self: Sized,
    {
        let db = open(path).map_err(|e| arcon_err_kind!("Could not open sled: {}", e))?;
        Ok(Sled { db })
    }

    fn checkpoint(&self, checkpoint_path: &str) -> ArconResult<()> {
        // TODO: sled doesn't support checkpoints/snapshots, but that and MVCC is planned
        //   for now we'll just dump it via the export/import mechanism WHICH MAY BE VERY SLOW
        let export_data = self.db.export();

        let mut p: PathBuf = checkpoint_path.into();
        fs::create_dir(&p).map_err(|e| arcon_err_kind!("Could not create checkpoint dir {}", e))?;

        p.push("SLED_EXPORT");
        let out = File::create(&p)
            .map_err(|e| arcon_err_kind!("Could not create checkpoint file {}", e))?;
        let mut writer = BufWriter::new(out);

        // TODO: the following section has a lot of io::Result-returning methods, and `try` expressions
        //   aren't stable yet, so let's do immediately-executed closure trick
        || -> io::Result<()> {
            writer.write_all(&export_data.len().to_le_bytes())?;

            fn write_len_and_bytes(mut w: impl Write, bytes: &[u8]) -> io::Result<()> {
                w.write_all(&bytes.len().to_le_bytes())?;
                w.write_all(bytes)?;
                Ok(())
            }

            for (typ, name, vvecs) in export_data {
                write_len_and_bytes(&mut writer, &typ)?;
                write_len_and_bytes(&mut writer, &name)?;

                // we don't know how many elements are in `vvecs`, so let's save the current position,
                // write a dummy length, write all the elements and then go back and write how many we wrote
                let length_spot = writer.seek(SeekFrom::Current(0))?;
                writer.write_all(&0usize.to_le_bytes())?;
                let mut length = 0usize;

                for vecs in vvecs {
                    length += 1;
                    writer.write_all(&vecs.len().to_le_bytes())?;
                    for vec in vecs {
                        write_len_and_bytes(&mut writer, &vec)?;
                    }
                }

                let after_vecs = writer.seek(SeekFrom::Current(0))?;
                let _ = writer.seek(SeekFrom::Start(length_spot))?;
                writer.write_all(&length.to_le_bytes())?;
                let _ = writer.seek(SeekFrom::Start(after_vecs))?;
            }
            Ok(())
        }()
        .map_err(|e| arcon_err_kind!("sled checkpoint io error, {}", e))?;

        Ok(())
    }

    fn restore(restore_path: &str, checkpoint_path: &str) -> ArconResult<Self>
    where
        Self: Sized,
    {
        let db = open(restore_path).map_err(|e| arcon_err_kind!("Could open sled, {}", e))?;
        let import_data = parse_dumped_sled_export(checkpoint_path)?;
        db.import(import_data);

        Ok(Sled { db })
    }

    fn just_restored(&mut self) -> bool {
        self.db.was_recovered()
    }
}

fn parse_dumped_sled_export(
    dump_path: &str,
) -> ArconResult<Vec<(Vec<u8>, Vec<u8>, impl Iterator<Item = Vec<Vec<u8>>>)>> {
    let f = File::open(dump_path).map_err(|e| arcon_err_kind!("Sled restore error, {}", e))?;
    let mut reader = BufReader::new(f);

    // immediately executed closure trick, TODO: use `try` expression when it stabilises
    let res = || -> io::Result<_> {
        #[inline]
        fn read_length(r: &mut impl Read) -> io::Result<usize> {
            let mut length_bytes = 0usize.to_le_bytes();
            r.read_exact(&mut length_bytes)?;
            Ok(usize::from_le_bytes(length_bytes))
        }

        let length = read_length(&mut reader)?;
        let mut res = Vec::with_capacity(length);

        #[inline]
        fn read_bytes_starting_with_len(r: &mut impl Read) -> io::Result<Vec<u8>> {
            let length = read_length(r)?;
            let mut res = vec![0u8; length];
            r.read_exact(&mut res)?;
            Ok(res)
        }

        // TODO: can we make it so this doesn't run out of memory when the database is huge?
        //   that could be possibly done by skipping around the file to read the type and name and
        //   then reading the actual contents (impl Iterator) lazily
        for _ in 0..length {
            let typ = read_bytes_starting_with_len(&mut reader)?;
            let name = read_bytes_starting_with_len(&mut reader)?;

            let vvecs_length = read_length(&mut reader)?;
            let mut vvecs = Vec::with_capacity(vvecs_length);
            for _ in 0..vvecs_length {
                let vecs_length = read_length(&mut reader)?;
                let mut vecs = Vec::with_capacity(vecs_length);
                for _ in 0..vecs_length {
                    vecs.push(read_bytes_starting_with_len(&mut reader)?);
                }
                vvecs.push(vecs);
            }
            let vvecs = vvecs.into_iter();

            res.push((typ, name, vvecs))
        }

        Ok(res)
    }()
    .map_err(|e| arcon_err_kind!("sled restore io error, {}", e))?;

    Ok(res)
}

// mod aggregating_state;
// mod map_state;
// mod reducing_state;
// mod value_state;
// mod vec_state;
