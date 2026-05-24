using Microsoft.EntityFrameworkCore;
using TrackingService.Models;

namespace TrackingService.Data;

public class TrackingDbContext : DbContext
{
    public TrackingDbContext(DbContextOptions<TrackingDbContext> options) : base(options) { }

    public DbSet<TrackingRecord> TrackingRecords => Set<TrackingRecord>();
    public DbSet<TrackingEvent> TrackingEvents => Set<TrackingEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TrackingRecord>(entity =>
        {
            entity.HasKey(t => t.Id);
            entity.HasIndex(t => t.OrderId).IsUnique();
            entity.HasIndex(t => t.OrderCode).IsUnique();
            entity.HasMany(t => t.Events)
                  .WithOne(e => e.TrackingRecord)
                  .HasForeignKey(e => e.TrackingRecordId)
                  .OnDelete(DeleteBehavior.Cascade);
        });
    }
}
